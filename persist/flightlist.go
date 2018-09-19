package persist

import (
	"database/sql"

	"time"

	"strings"

	"fmt"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/seeds"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/umetrip/parser"
	_ "github.com/denisenkom/go-mssqldb"
)

var (
	db *sql.DB
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
}

var airportIndex = 0
var flightSum = 0

func Print(result types.ParseResult, notifier types.PrintNotifier,
	limiter types.RateLimiter) bool {

	var itemCount = 0
	for _, item := range result.Items {
		_ = item.(parser.FlightListData)

		itemCount++
		flightSum++
	}
	airportIndex++

	data := types.NotifyData{
		Type:    "list",
		Elapsed: time.Since(types.T1),
		Date:    result.Request.RawParam.Date,
		Airport: types.Airport{
			DepCode: result.Request.RawParam.Dep,
			ArrCode: result.Request.RawParam.Arr},
		AirportIndex: airportIndex,
		AirportTotal: seeds.TotalAirports,
		FlightCount:  itemCount,
		FlightSum:    flightSum,
		Progress:     float32(100 * float64(airportIndex) / float64(seeds.TotalAirports)),
		QPS:          limiter.QPS(),
	}
	notifier.Print(data)

	// task is completed?
	if airportIndex >= seeds.TotalAirports {
		return true
	} else {
		return false
	}
}

func Save(result types.ParseResult, notifier types.PrintNotifier, limiter types.RateLimiter) (
	parser.FlightListData, bool, error) {

	//create table to save result
	date := strings.Replace(result.Request.RawParam.Date, "-", "", -1)[0:6]
	_, err := db.Exec("sp_createFutureListTable", sql.Named("tablename", "dbo.FutureList_"+date))
	if err != nil {
		panic(err)
	}

	var itemCount = 0
	for _, item := range result.Items {
		data := item.(parser.FlightListData)
		split := strings.Split(data.Airport, "/")

		_, err := db.Exec("insert into [dbo].[FutureList_" + date + "]" +
			"(dep,arr,date,flightNo,flightName,flightState,depPlanTime,arrPlanTime,depActualTime," +
			"arrActualTime,depPort,arrPort,createAt)" +
			" values ('" + result.Request.RawParam.Dep + "', '" + result.Request.RawParam.Arr + "', '" + result.Request.RawParam.Date +
			"', '" + data.FlightNo + "', '" + data.FlightCompany + "', '" + data.State +
			"', '" + (result.Request.RawParam.Date + " " + data.DepTimePlan) +
			"', '" + fixarrdate(result.Request.RawParam.Date, data.DepTimePlan, data.ArrTimePlan) +
			"', '" + strings.Replace(data.DepTimeActual, "-", "", -1) +
			"', '" + strings.Replace(data.ArrTimeActual, "-", "", -1) +
			"', '" + strings.Replace(split[0], "-", "", -1) +
			"', '" + strings.Replace(split[1], "-", "", -1) +
			"', '" + time.Now().Format("2006-01-02 15:04:05") + "')")
		if err != nil {
			return data, false, err
		}

		itemCount++
		flightSum++
	}

	airportIndex++

	data := types.NotifyData{
		Type:    "list",
		Elapsed: time.Since(types.T1),
		Date:    result.Request.RawParam.Date,
		Airport: types.Airport{
			DepCode: result.Request.RawParam.Dep,
			ArrCode: result.Request.RawParam.Arr},
		AirportIndex: airportIndex,
		AirportTotal: seeds.TotalAirports,
		FlightCount:  itemCount,
		FlightSum:    flightSum,
		Progress:     float32(100 * float64(airportIndex) / float64(seeds.TotalAirports)),
		QPS:          limiter.QPS(),
	}
	notifier.Print(data)

	// task is completed?
	if airportIndex >= seeds.TotalAirports {
		return parser.FlightListData{}, true, nil
	} else {
		return parser.FlightListData{}, false, nil
	}
}

func fixarrdate(date, deptime, arrtime string) string {
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
