package persist

import (
	"database/sql"
	"fmt"

	"time"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/umetrip/parser"
	_ "github.com/denisenkom/go-mssqldb"
)

func init() {
	var err error

	// 连接到 FlightData 数据库
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s&connection+timeout=10",
		config.SqlUser, config.SqlPass, config.SqlAddr, "FlightData")
	db, err = sql.Open("sqlserver", connstr)
	if err != nil {
		panic(err)
	}

	StatData = make(map[string]*StatEntry)
}

type StatEntry struct {
	Date         string
	CompletedSum int
	SuspendSum   int
	NotFound     int
}

var StatData map[string]*StatEntry

func PrintRealTime(result types.ParseResult, limiter types.RateLimiter,
	reqChan chan types.Request) bool {

	isfinished := true
	for _, item := range result.Items {
		data := item.(parser.FlightDetailData)
		if IsFinished(data.FlightState) == false {
			isfinished = false
			break
		}
	}

	data, ok := StatData[result.Request.RawParam.Date]
	if !ok {
		data = &StatEntry{
			Date: result.Request.RawParam.Date,
		}
		StatData[result.Request.RawParam.Date] = data
	}

	fmt.Printf("\r#%s [RealTime chanCAP:%d %d/%d/%d] [Rate:%.2fqps]",
		data.Date, len(reqChan), data.CompletedSum, data.SuspendSum,
		data.NotFound, limiter.QPS())

	if isfinished == false {
		data.SuspendSum++
		go SaveRealTime(result)
		return false
	} else {
		if len(result.Items) == 0 {
			data.NotFound++
		} else {
			data.CompletedSum++
			go SaveRealTime(result)
		}
		return true
	}
}

func IsFinished(state string) bool {
	if state == "到达" || state == "取消" || state == "备降" || state == "返航" {
		return true
	}
	return false
}

func SaveRealTime(result types.ParseResult) error {
	for _, item := range result.Items {
		data := item.(parser.FlightDetailData)

		var state string
		// 检查航班状态是否有变化
		db.QueryRow(fmt.Sprintf(
			"select top 1 flightState from [dbo].[RealTime] "+
				"where flightNo='%s' and date='%s' and depCode='%s' and arrCode='%s' "+
				"order by createAt desc",
			data.FlightNo, data.FlightDate, data.DepCode, data.ArrCode)).Scan(&state)

		if data.FlightState == state {
			// 状态没有发生变化
			return nil
		}
		// 解析起降时间
		depPlanTime := parseTimeCode(data.DepPlanTime)
		depActualTime := parseTimeCode(data.DepActualTime)
		arrPlanTime := parseTimeCode(data.ArrPlanTime)
		arrActualTime := parseTimeCode(data.ArrActualTime)

		_, err := db.Exec("insert into [dbo].[RealTime]" +
			"(flightNo,date,depCode,arrCode,depCity,arrCity,flightState,code1,code2,code3," +
			"code4,depPlanTime,depActualTime,arrPlanTime,arrActualTime,mileage,duration,age," +
			"preFlightNo,preFlightState,preFlightDepCode,preFlightArrCode," +
			"depWeather,arrWeather,depFlow,arrFlow,createAt)" +
			" values ('" + data.FlightNo + "', '" + data.FlightDate +
			"', '" + data.DepCode + "', '" + data.ArrCode +
			"', '" + data.DepCity + "', '" + data.ArrCity +
			"', '" + data.FlightState +
			"', '" + data.DepPlanTime +
			"', '" + data.DepActualTime +
			"', '" + data.ArrPlanTime +
			"', '" + data.ArrActualTime +
			"', '" + timeToDatetime(data.FlightDate, depPlanTime, depPlanTime) +
			"', '" + timeToDatetime(data.FlightDate, depPlanTime, depActualTime) +
			"', '" + timeToDatetime(data.FlightDate, depPlanTime, arrPlanTime) +
			"', '" + timeToDatetime(data.FlightDate, depPlanTime, arrActualTime) +
			"', '" + data.Mileage + "', '" + data.Duration + "', '" + data.Age +
			"', '" + data.PreFlightNo + "', '" + data.PreFlightState +
			"', '" + data.PreFlightDepCode +
			"', '" + data.PreFlightArrCode +
			"', '" + data.DepWeather + "', '" + data.ArrWeather +
			"', '" + data.DepFlow + "', '" + data.ArrFlow +
			"', '" + time.Now().Format("2006-01-02 15:04:05") + "')")
		if err != nil {
			return err
		}
	}
	return nil
}
