package persist

import (
	"database/sql"

	"time"

	"fmt"

	"strings"

	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/umetrip/parser"
	_ "github.com/denisenkom/go-mssqldb"
)

type Saver struct {
	parseResultChan chan types.ParseResult
}

func (s *Saver) ConfigureParseResultChan(ch chan types.ParseResult) {
	s.parseResultChan = ch
}

func (s *Saver) Submit(result types.ParseResult) {
	go func() {
		s.parseResultChan <- result
	}()
}

var (
	conn *sql.DB
)

func init() {
	// connect sql
	var err error
	conn, err = sql.Open("mssql",
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
		Airport:      types.Airport{DepCode: result.Dep, ArrCode: result.Arr},
		AirportIndex: airportIndex,
		FlightCount:  itemCount,
		FlightSum:    flightSum,
		Progress:     float32(100 * float64(airportIndex) / 49948),
	}

	notifier.Print(data)
}

func Save(result types.ParseResult) (parser.FlightListData, error) {
	for _, item := range result.Items {
		data := item.(parser.FlightListData)
		split := strings.Split(data.Airport, "/")
		_, err := conn.Exec("insert into [dbo].[Airline_20180907]" +
			"(dep,arr,date,flightNo,flightName,flightState,depPlanTime,arrPlanTime," +
			"depActualTime,arrActualTime,depPort,arrPort)" +
			" values ('" + result.Dep + "', '" + result.Arr + "', '" + result.Date + "', '" + data.FlightNo + "', '" + data.FlightCompany + "'," +
			" '" + data.State + "','" + data.DepTimePlan + "', '" + data.ArrTimePlan + "', '" + data.DepTimeActual + "'," +
			" '" + data.ArrTimeActual + "', '" + split[0] + "', '" + split[1] + "')")
		if err != nil {
			return data, err
		}
	}
	airportIndex++
	fmt.Printf("\rSave airport #%d", airportIndex)
	return parser.FlightListData{}, nil
}
