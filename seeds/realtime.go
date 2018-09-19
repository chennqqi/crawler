package seeds

import (
	"database/sql"
	"fmt"
	"log"

	"time"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/umetrip/parser"
	_ "github.com/denisenkom/go-mssqldb"
)

func PullLatestFlight(container chan types.Request) error {
	// connect sql server
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s&connection+timeout=60",
		config.SqlUser, config.SqlPass, config.SqlAddr, "FlightData")
	db, err := sql.Open("sqlserver", connstr)
	if err != nil {
		return err
	}

	// 计算日期
	var tablename = time.Now().Format("200601")
	var now = time.Now().Format("2006-01-02 15:04:05")
	var end = time.Now().Add(2 * time.Hour).Format("2006-01-02 15:04:05")

	var query = fmt.Sprintf("select date,flightNo from [Airline_%s] "+
		"where depPlanTime >= '%s' "+
		"and depPlanTime <= '%s' "+
		"order by depPlanTime", tablename, now, end)

	go func() {
		rows, err := db.Query(query)
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		var flight Flight
		for rows.Next() {
			err := rows.Scan(&flight.FlightDate, &flight.FlightNo)
			if err != nil {
				log.Fatal(err)
			}

			// 详情页url:http://www.umetrip.com/mskyweb/fs/fc.do?flightNo=MU3924&date=2018-09-13
			url := fmt.Sprintf("http://www.umetrip.com/mskyweb/fs/fc.do?flightNo=%s&date=%s",
				flight.FlightNo, flight.FlightDate)

			container <- types.Request{
				Url:        url,
				ParserFunc: parser.ParseDetail,
				RawParam: types.Param{
					Date: flight.FlightDate,
					Fno:  flight.FlightNo,
				},
			}
		}
	}()

	return nil
}
