package persist

import (
	"strings"

	"fmt"
	"os"

	"database/sql"

	"time"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/seeds"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/umetrip/parser"
	"github.com/champkeh/crawler/utils"
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

func ClearDataBase() {
	// 清除之前的数据
	_, err := db.Exec("delete from [dbo].[FutureDetail_" + time.Now().Add(24*time.Hour).Format("200601") + "] " +
		"where date='" + time.Now().Add(24*time.Hour).Format("2006-01-02") + "'")
	if err != nil {
		panic(err)
	}
}

func SaveDetail(result types.ParseResult, notifier types.PrintNotifier, limiter types.RateLimiter) (
	parser.FlightDetailData, error) {

	// 确保数据表存在
	tabledate := strings.Replace(result.Request.RawParam.Date, "-", "", -1)[0:6]
	_, err := db.Exec("sp_createFutureDetailTable", sql.Named("tablename", "FutureDetail_"+tabledate))
	if err != nil {
		panic(err)
	}

	var itemCount = 0

	for _, item := range result.Items {
		data := item.(parser.FlightDetailData)

		// 解析起降时间
		depPlanTime := utils.Code2Time(data.DepPlanTime)
		depActualTime := utils.Code2Time(data.DepActualTime)
		arrPlanTime := utils.Code2Time(data.ArrPlanTime)
		arrActualTime := utils.Code2Time(data.ArrActualTime)

		_, err := db.Exec("insert into [dbo].[FutureDetail_" + tabledate + "]" +
			"(flightNo,date,depCode,arrCode,depCity,arrCity,flightState," +
			"depPlanTime,depActualTime,arrPlanTime,arrActualTime," +
			"code1,code2,code3,code4," +
			"mileage,duration,age," +
			"preFlightNo,preFlightState,preFlightDepCode,preFlightArrCode," +
			"depWeather,arrWeather," +
			"depFlow,arrFlow,createAt)" +
			" values ('" + data.FlightNo + "', '" + data.FlightDate +
			"', '" + data.DepCode + "', '" + data.ArrCode +
			"', '" + data.DepCity + "', '" + data.ArrCity +
			"', '" + data.FlightState +
			"', '" + utils.TimeToDatetime(data.FlightDate, depPlanTime, depPlanTime) +
			"', '" + utils.TimeToDatetime(data.FlightDate, depPlanTime, depActualTime) +
			"', '" + utils.TimeToDatetime(data.FlightDate, depPlanTime, arrPlanTime) +
			"', '" + utils.TimeToDatetime(data.FlightDate, depPlanTime, arrActualTime) +
			"', '" + data.DepPlanTime +
			"', '" + data.DepActualTime +
			"', '" + data.ArrPlanTime +
			"', '" + data.ArrActualTime +
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
