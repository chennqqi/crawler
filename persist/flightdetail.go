package persist

import (
	"strings"

	"fmt"
	"os"

	"database/sql"

	"time"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/ocr"
	"github.com/champkeh/crawler/seeds"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/umetrip/parser"
	_ "github.com/denisenkom/go-mssqldb"
)

func PrintDetail(result types.ParseResult, notifier types.PrintNotifier,
	limiter types.RateLimiter) {

	var itemCount = 0
	for _, item := range result.Items {
		_ = item.(parser.FlightDetailData)

		itemCount++
	}
	FlightSum++

	data := types.NotifyData{
		Type:        "detail",
		Elapsed:     time.Since(types.T1),
		Date:        result.Request.RawParam.Date,
		FlightCount: itemCount,
		FlightSum:   FlightSum,
		FlightTotal: seeds.TotalFlight,
		Progress:    float32(100 * float64(FlightSum) / float64(seeds.TotalFlight)),
		QPS:         limiter.QPS(),
	}
	notifier.Print(data)

	// task is completed?
	if FlightSum >= seeds.TotalFlight {
		go func() {
			// program exit after 5 seconds
			fmt.Println("Completed! Program will exit after 5 seconds...")
			time.Sleep(5 * time.Second)
			os.Exit(0)
		}()
	}
}

func init() {
	var err error
	// 连接到 FlightData 数据库
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s&connection+timeout=60",
		config.SqlUser, config.SqlPass, config.SqlAddr, "FlightData")
	db, err = sql.Open("sqlserver", connstr)
	if err != nil {
		panic(err)
	}
}

func SaveDetail(result types.ParseResult, notifier types.PrintNotifier, limiter types.RateLimiter) (
	parser.FlightDetailData, error) {

	// 确保数据表存在
	date := strings.Replace(result.Request.RawParam.Date, "-", "", -1)[0:6]
	_, err := db.Exec("sp_createFutureDetailTable", sql.Named("tablename", "FutureDetail_"+date))
	if err != nil {
		panic(err)
	}

	var itemCount = 0

	for _, item := range result.Items {
		data := item.(parser.FlightDetailData)

		// 解析起降时间
		depPlanTime := parseTimeCode(data.DepPlanTime)
		depActualTime := parseTimeCode(data.DepActualTime)
		arrPlanTime := parseTimeCode(data.ArrPlanTime)
		arrActualTime := parseTimeCode(data.ArrActualTime)

		_, err := db.Exec("insert into [dbo].[FutureDetail_" + date + "]" +
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
			return data, err
		}

		itemCount++

	}
	FlightSum++

	data := types.NotifyData{
		Type:        "detail",
		Elapsed:     time.Since(types.T1),
		Date:        result.Request.RawParam.Date,
		FlightCount: itemCount,
		FlightSum:   FlightSum,
		FlightTotal: seeds.TotalFlight,
		Progress:    float32(100 * float64(FlightSum) / float64(seeds.TotalFlight)),
		QPS:         limiter.QPS(),
	}
	notifier.Print(data)

	// task is completed?
	if FlightSum >= seeds.TotalFlight {
		go func() {
			// program exit after 5 seconds
			fmt.Println("Completed! Program will exit after 5 seconds...")
			time.Sleep(5 * time.Second)
			os.Exit(0)
		}()
	}

	return parser.FlightDetailData{}, nil
}

func parseTimeCode(code string) string {
	// 查数据库
	s, err := ocr.CodeToTime(code)
	if err != nil {
		// 数据库命中
		return ""
	}

	return s
}

func timeToDatetime(date, deptime, arrtime string) string {
	if deptime == "" || arrtime == "" {
		return "1990-01-01 00:00"
	}

	if arrtime >= deptime {
		return date + " " + arrtime
	} else {
		parse, err := time.Parse("2006-01-02", date)
		if err != nil {
			panic(err)
		}
		return parse.Add(24*time.Hour).Format("2006-01-02") + " " + arrtime
	}
}
