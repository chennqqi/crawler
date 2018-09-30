package persist

import (
	"database/sql"
	"fmt"

	"time"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/umetrip/parser"
	"github.com/champkeh/crawler/utils"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/labstack/gommon/log"
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
	reqChan chan types.Request) (bool, string) {

	isFinished := true
	state := ""
	for _, item := range result.Items {
		flightItem := item.(parser.FlightDetailData)
		state = flightItem.FlightState
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
		return false, state
	} else {
		if len(result.Items) == 0 {
			data.NotFound++
		} else {
			data.CompletedSum++
			go SaveRealTime(result)
		}
		return true, state
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
	DepPlanTime    string
	DepActualTime  string
	ArrPlanTime    string
	ArrActualTime  string
	PreFlightNo    string
	PreFlightState string
}

func SaveRealTime(result types.ParseResult) error {
	for _, item := range result.Items {
		data := item.(parser.FlightDetailData)

		// 将code解析为time
		depPlanTime := utils.Code2Time(data.DepPlanTime)
		depActualTime := utils.Code2Time(data.DepActualTime)
		arrPlanTime := utils.Code2Time(data.ArrPlanTime)
		arrActualTime := utils.Code2Time(data.ArrActualTime)

		// 将time转为datetime
		data.DepPlanTime = utils.TimeToDatetime(data.FlightDate, depPlanTime, depPlanTime)
		data.DepActualTime = utils.TimeToDatetime(data.FlightDate, depPlanTime, depActualTime)
		data.ArrPlanTime = utils.TimeToDatetime(data.FlightDate, depPlanTime, arrPlanTime)
		data.ArrActualTime = utils.TimeToDatetime(data.FlightDate, depPlanTime, arrActualTime)

		// 获取数据库中该航班的最新状态并进行比较
		var dbFlightState dbFlight
		err := db.QueryRow(fmt.Sprintf(
			"select top 1 "+
				"flightNo,date,depCode,arrCode,flightState,"+
				"depPlanTime,depActualTime,arrPlanTime,arrActualTime,"+
				"preFlightNo,preFlightState "+
				"from [dbo].[RealTime] "+
				"where flightNo='%s' and date='%s' and depCode='%s' and arrCode='%s' "+
				"order by updateAt desc",
			data.FlightNo, data.FlightDate, data.DepCode, data.ArrCode)).Scan(
			&dbFlightState.FlightNo,
			&dbFlightState.Date,
			&dbFlightState.DepCode,
			&dbFlightState.ArrCode,
			&dbFlightState.FlightState,
			&dbFlightState.DepPlanTime,
			&dbFlightState.DepActualTime,
			&dbFlightState.ArrPlanTime,
			&dbFlightState.ArrActualTime,
			&dbFlightState.PreFlightNo,
			&dbFlightState.PreFlightState)

		if Equal(data, dbFlightState) {
			// 状态没有发生任何变化，该航班不需要保存
			continue
		}

		if err == sql.ErrNoRows {
			log.Warnf("not exist entry [%s:%s:%s:%s]",
				data.FlightDate, data.FlightNo, data.DepCode, data.ArrCode)

			// 只有当天的航班，才进行插入操作
			// 因为之前的不存在的航班，可能是已经归档了
			if data.FlightDate >= time.Now().Format("2006-01-02") {
				// 插入
				_, err = db.Exec(fmt.Sprintf("insert into [dbo].[RealTime]"+
					"(flightNo,date,depCode,arrCode,depCity,arrCity,flightState,"+
					"depPlanTime,depActualTime,arrPlanTime,arrActualTime,"+
					"mileage,duration,age,"+
					"preFlightNo,preFlightState,preFlightDepCode,preFlightArrCode,"+
					"depWeather,arrWeather,"+
					"depFlow,arrFlow,updateAt)"+
					" values ("+
					"'%s','%s','%s','%s','%s','%s','%s',"+
					"'%s','%s','%s','%s',"+
					"'%s','%s','%s',"+
					"'%s','%s','%s','%s',"+
					"'%s','%s',"+
					"'%s','%s','%s')",
					data.FlightNo,
					data.FlightDate,
					data.DepCode,
					data.ArrCode,
					data.DepCity,
					data.ArrCity,
					data.FlightState,
					data.DepPlanTime,
					data.DepActualTime,
					data.ArrPlanTime,
					data.ArrActualTime,
					data.Mileage,
					data.Duration,
					data.Age,
					data.PreFlightNo,
					data.PreFlightState,
					data.PreFlightDepCode,
					data.PreFlightArrCode,
					data.DepWeather,
					data.ArrWeather,
					data.DepFlow,
					data.ArrFlow,
					time.Now().Format("2006-01-02 15:04:05")))
				if err != nil {
					log.Fatalf("insert error: %s", err)
				}
			}
		} else if err == nil {
			// 更新数据字段

			// 判断状态是否合理
			// 暂无<计划<起飞<到达
			// 预警<计划<起飞<到达
			if data.FlightState == "暂无" && dbFlightState.FlightState != "暂无" {
				continue
			}

			_, err = db.Exec(fmt.Sprintf("update [dbo].[RealTime]"+
				" set"+
				" flightState='%s',"+
				" depPlanTime='%s',"+
				" depActualTime='%s',"+
				" arrPlanTime='%s',"+
				" arrActualTime='%s',"+
				" mileage='%s',"+
				" duration='%s',"+
				" age='%s',"+
				" preFlightNo='%s',"+
				" preFlightState='%s',"+
				" preFlightDepCode='%s',"+
				" preFlightArrCode='%s',"+
				" depWeather='%s',"+
				" arrWeather='%s',"+
				" depFlow='%s',"+
				" arrFlow='%s',"+
				" updateAt='%s'"+
				" where flightNo='%s' and date='%s' and depCode='%s' and arrCode='%s'",
				data.FlightState,
				data.DepPlanTime,
				data.DepActualTime,
				data.ArrPlanTime,
				data.ArrActualTime,
				data.Mileage,
				data.Duration,
				data.Age,
				data.PreFlightNo,
				data.PreFlightState,
				data.PreFlightDepCode,
				data.PreFlightArrCode,
				data.DepWeather,
				data.ArrWeather,
				data.DepFlow,
				data.ArrFlow,
				time.Now().Format("2006-01-02 15:04:05"),
				data.FlightNo, data.FlightDate, data.DepCode, data.ArrCode))
			if err != nil {
				log.Fatalf("update error: %s", err)
			}

			// 更新抓取时间字段
			if data.FlightState == "起飞" && dbFlightState.FlightState != "起飞" {
				_, err = db.Exec(fmt.Sprintf("update [dbo].[RealTime]"+
					" set"+
					" depAt='%s'"+
					" where flightNo='%s' and date='%s' and depCode='%s' and arrCode='%s'",
					time.Now().Format("2006-01-02 15:04:05"),
					data.FlightNo, data.FlightDate, data.DepCode, data.ArrCode))
				if err != nil {
					log.Warnf("update depAt error: %v", err)
				}
			} else if data.FlightState == "到达" && dbFlightState.FlightState != "到达" {
				_, err = db.Exec(fmt.Sprintf("update [dbo].[RealTime]"+
					" set"+
					" arrAt='%s'"+
					" where flightNo='%s' and date='%s' and depCode='%s' and arrCode='%s'",
					time.Now().Format("2006-01-02 15:04:05"),
					data.FlightNo, data.FlightDate, data.DepCode, data.ArrCode))
				if err != nil {
					log.Warnf("update arrAt error: %v", err)
				}
			}
		} else {
			log.Warnf("scan dbflight error: %v", err)
		}
	}
	return nil
}

func Equal(newdata parser.FlightDetailData, olddata dbFlight) bool {
	// 比较时间
	if olddata.DepPlanTime != newdata.DepPlanTime || olddata.DepActualTime != newdata.DepActualTime ||
		olddata.ArrPlanTime != newdata.ArrPlanTime || olddata.ArrActualTime != newdata.ArrActualTime {
		return false
	}

	// 比较航班状态
	if olddata.FlightState != newdata.FlightState {
		return false
	}

	// 比较前序航班状态
	if olddata.PreFlightNo != newdata.PreFlightNo || olddata.PreFlightState != newdata.PreFlightState {
		return false
	}
	return true
}
