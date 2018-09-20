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

type StatEntry struct {
	Date         string
	CompletedSum int
	SuspendSum   int
	NotFound     int
}

var StatData map[string]*StatEntry

func init() {
	var err error

	// 连接到 FlightData 数据库
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s&connection+timeout=10",
		config.SqlUser, config.SqlPass, config.SqlAddr, "FlightData")
	db, err = sql.Open("sqlserver", connstr)
	if err != nil {
		panic(err)
	}

	// 初始化统计容器
	StatData = make(map[string]*StatEntry)
}

func PrintRealTime(result types.ParseResult, limiter types.RateLimiter,
	reqChan chan types.Request) bool {

	isFinished := true
	for _, item := range result.Items {
		flightItem := item.(parser.FlightDetailData)
		if IsFinished(flightItem.FlightState) == false {
			isFinished = false
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

	if isFinished == false {
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

type dbFlight struct {
	FlightNo       string
	Date           string
	DepCode        string
	ArrCode        string
	FlightState    string
	Code1          string
	Code2          string
	Code3          string
	Code4          string
	PreFlightNo    string
	PreFlightState string
}

func SaveRealTime(result types.ParseResult) error {
	for _, item := range result.Items {
		data := item.(parser.FlightDetailData)

		// 获取数据库中该航班的最新状态并进行比较
		var dbFlightState dbFlight
		db.QueryRow(fmt.Sprintf(
			"select top 1 flightNo,date,depCode,arrCode,flightState,code1,code2,code3,code4,preFlightNo,preFlightState from [dbo].[RealTime] "+
				"where flightNo='%s' and date='%s' and depCode='%s' and arrCode='%s' "+
				"order by createAt desc",
			data.FlightNo, data.FlightDate, data.DepCode, data.ArrCode)).Scan(
			&dbFlightState.FlightNo,
			&dbFlightState.Date,
			&dbFlightState.DepCode,
			&dbFlightState.ArrCode,
			&dbFlightState.FlightState,
			&dbFlightState.Code1,
			&dbFlightState.Code2,
			&dbFlightState.Code3,
			&dbFlightState.Code4,
			&dbFlightState.PreFlightNo,
			&dbFlightState.PreFlightState)

		if Equal(data, dbFlightState) {
			// 状态没有发生任何变化，该航班不需要保存
			continue
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

func Equal(newdata parser.FlightDetailData, old dbFlight) bool {
	// 比较时间
	if old.Code1 != newdata.DepPlanTime || old.Code2 != newdata.DepActualTime ||
		old.Code3 != newdata.ArrPlanTime || old.Code4 != newdata.ArrActualTime {
		return false
	}

	// 比较航班状态
	if old.FlightState != newdata.FlightState {
		return false
	}

	// 比较前序航班状态
	if old.PreFlightNo != newdata.PreFlightNo || old.PreFlightState != newdata.PreFlightState {
		return false
	}
	return true
}
