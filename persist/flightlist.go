package persist

import (
	"database/sql"

	"time"

	"strings"

	"fmt"
	"os"

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
	limiter types.RateLimiter) {

	var itemCount = 0
	for _, item := range result.Items {
		_ = item.(parser.FlightListData)

		itemCount++
		flightSum++
	}
	airportIndex++

	data := types.NotifyData{
		Type:         "v1",
		Elapsed:      time.Since(types.T1),
		Airport:      types.Airport{DepCode: result.RawParam.Dep, ArrCode: result.RawParam.Arr},
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
		go func() {
			// program exit after 5 seconds
			fmt.Println("Completed! Program will exit after 5 seconds...")
			time.Sleep(5 * time.Second)
			os.Exit(0)
		}()
	}
}

func Save(result types.ParseResult, notifier types.PrintNotifier, limiter types.RateLimiter) (
	parser.FlightListData, error) {

	//create table to save result
	date := strings.Replace(result.RawParam.Date, "-", "", -1)[0:6]
	_, err := db.Exec("sp_createTable", sql.Named("tablename", "Airline_"+date))
	if err != nil {
		panic(err)
	}

	var itemCount = 0
	for _, item := range result.Items {
		data := item.(parser.FlightListData)
		split := strings.Split(data.Airport, "/")

		_, err := db.Exec("insert into [dbo].[Airline_" + date + "]" +
			"(dep,arr,date,flightNo,flightName,flightState,depPlanTime,arrPlanTime,depActualTime," +
			"arrActualTime,depPort,arrPort,createAt)" +
			" values ('" + result.RawParam.Dep + "', '" + result.RawParam.Arr + "', '" + result.RawParam.Date +
			"', '" + data.FlightNo + "', '" + data.FlightCompany + "', '" + data.State +
			"', '" + (result.RawParam.Date + " " + data.DepTimePlan) +
			"', '" + fixarrdate(result.RawParam.Date, data.DepTimePlan, data.ArrTimePlan) +
			"', '" + strings.Replace(data.DepTimeActual, "-", "", -1) +
			"', '" + strings.Replace(data.ArrTimeActual, "-", "", -1) +
			"', '" + strings.Replace(split[0], "-", "", -1) +
			"', '" + strings.Replace(split[1], "-", "", -1) +
			"', '" + time.Now().Format("2006-01-02 15:04:05") + "')")
		if err != nil {
			return data, err
		}

		itemCount++
		flightSum++
	}

	airportIndex++

	data := types.NotifyData{
		Type:         "v1",
		Elapsed:      time.Since(types.T1),
		Airport:      types.Airport{DepCode: result.RawParam.Dep, ArrCode: result.RawParam.Arr},
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
		go func() {
			// program exit after 5 seconds
			fmt.Println("Completed! Program will exit after 5 seconds...")
			time.Sleep(5 * time.Second)
			os.Exit(0)
		}()
	}

	return parser.FlightListData{}, nil
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
