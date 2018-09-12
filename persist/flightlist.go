package persist

import (
	"database/sql"

	"time"

	"strings"

	"fmt"
	"os"

	"github.com/champkeh/crawler/seeds"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/umetrip/parser"
	_ "github.com/denisenkom/go-mssqldb"
)

var (
	db *sql.DB
)

func init() {
	// connect sql server
	var err error
	db, err = sql.Open("sqlserver",
		"sqlserver://sa:123456@localhost:1433?database=data&connection+timeout=10")
	if err != nil {
		panic(err)
	}
}

var airportIndex = 0
var flightSum = 0

func Print(result types.ParseResult, notifier types.PrintNotifier) {
	var itemCount = 0
	for _, item := range result.Items {
		_ = item.(parser.FlightListData)

		itemCount++
		flightSum++
	}
	airportIndex++

	data := types.NotifyData{
		Elapsed:      time.Since(types.T1),
		Airport:      types.Airport{DepCode: result.RawParam.Dep, ArrCode: result.RawParam.Arr},
		AirportIndex: airportIndex,
		FlightCount:  itemCount,
		FlightSum:    flightSum,
		Progress:     float32(100 * float64(airportIndex) / float64(seeds.TotalAirports)),
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

func Save(result types.ParseResult, notifier types.PrintNotifier) (
	parser.FlightListData, error) {

	date := strings.Replace(result.RawParam.Date, "-", "", -1)[0:6]
	var itemCount = 0

	for _, item := range result.Items {
		data := item.(parser.FlightListData)
		split := strings.Split(data.Airport, "/")

		_, err := db.Exec("insert into [dbo].[Airline_" + date + "]" +
			"(dep,arr,date,flightNo,flightName,flightState,depPlanTime,arrPlanTime,depActualTime," +
			"arrActualTime,depPort,arrPort)" +
			" values ('" + result.RawParam.Dep + "', '" + result.RawParam.Arr + "', '" + result.RawParam.Date +
			"', '" + data.FlightNo + "', '" + data.FlightCompany + "', '" + data.State + "', '" + data.DepTimePlan +
			"', '" + data.ArrTimePlan + "', '" + data.DepTimeActual + "', '" + data.ArrTimeActual +
			"', '" + split[0] + "', '" + split[1] + "')")
		if err != nil {
			return data, err
		}

		itemCount++
		flightSum++
	}

	airportIndex++

	data := types.NotifyData{
		Elapsed:      time.Since(types.T1),
		Airport:      types.Airport{DepCode: result.RawParam.Dep, ArrCode: result.RawParam.Arr},
		AirportIndex: airportIndex,
		FlightCount:  itemCount,
		FlightSum:    flightSum,
		Progress:     float32(100 * float64(airportIndex) / float64(seeds.TotalAirports)),
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
