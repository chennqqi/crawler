package persist

import (
	"database/sql"

	"fmt"

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

var itemCount = 0

func Save(result types.ParseResult) error {
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

		fmt.Printf("Save item #%d: %v\n", itemCount, item)
		itemCount++
	}
	return nil
}
