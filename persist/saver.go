package persist

import (
	"database/sql"

	"fmt"

	"time"

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

func Save(result types.ParseResult) error {
	var itemCount = 0
	for _, item := range result.Items {
		_ = item.(parser.FlightListData)
		//_, err := conn.Exec("insert into [dbo].[Airline_20180907]" +
		//	"(dep,arr,date,flightNo,flightName,flightState,depPlanTime,arrPlanTime," +
		//	"depActualTime,arrActualTime,depPort,arrPort)" +
		//	" values ('" + result.Dep + "', '" + result.Arr + "', '" + result.Date + "', '" + data.FlightNo + "', '" + data.FlightCompany + "'," +
		//	" '" + data.State + "','" + data.DepTimePlan + "', '" + data.ArrTimePlan + "', '" + data.DepTimeActual + "'," +
		//	" '" + data.ArrTimeActual + "', '" + data.Airport + "', '" + data.Airport + "')")
		//if err != nil {
		//	return err
		//}

		//fmt.Printf("Save item #%d: %v\n", itemCount, item)
		itemCount++
		flightSum++
	}
	airportIndex++
	fmt.Printf("\r%v Airport #%d (%s->%s): items %d; total: %d/%.2f%%", time.Since(types.T1),
		airportIndex, result.Dep, result.Arr, itemCount, flightSum,
		float32(100*float64(airportIndex)/49948))
	return nil
}
