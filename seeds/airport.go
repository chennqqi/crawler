package seeds

import (
	"database/sql"

	"log"

	"fmt"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/umetrip/parser"
	_ "github.com/denisenkom/go-mssqldb"
)

// PullAirportList pull all airport data e.g. (PEK SHA),(SHA LYA) etc from database
func PullAirportList() (chan types.Airport, error) {

	// connect sql
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s&connection+timeout=10",
		config.SqlUser, config.SqlPass, config.SqlAddr, config.Database)

	conn, err := sql.Open("mssql", connstr)
	if err != nil {
		return nil, err
	}

	// test connection
	_, err = conn.Query("select top 1 * from dbo.Inf_AirportSTD")
	if err != nil {
		return nil, err
	}

	ch := make(chan types.Airport)

	go func() {
		rows, err := conn.Query(`select a.Code,b.Code from dbo.Inf_AirportSTD a
				join dbo.Inf_AirportSTD b on a.CityCode != b.CityCode`)
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		var airport types.Airport
		for rows.Next() {
			err := rows.Scan(&airport.DepCode, &airport.ArrCode)
			if err != nil {
				log.Fatal(err)
			}
			ch <- airport
		}
		close(ch)
	}()

	return ch, nil
}

func AirportRequestFilter(airports chan types.Airport) chan types.Request {
	requests := make(chan types.Request)
	go func() {
		for airport := range airports {
			// TODO: date 暂时写死
			date := "2018-09-09"
			url := fmt.Sprintf("http://www.umetrip.com/mskyweb/fs/fa.do?dep=%s&arr=%s&date=%s",
				airport.DepCode, airport.ArrCode, date)

			requests <- types.Request{
				Dep:        airport.DepCode,
				Arr:        airport.ArrCode,
				Date:       date,
				Url:        url,
				ParserFunc: parser.ParseList,
			}
		}

		close(requests)
	}()

	return requests
}
