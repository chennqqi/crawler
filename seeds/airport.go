package seeds

import (
	"database/sql"

	"log"

	"fmt"

	"time"

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

	// this channel is non-buffer channel, which means that send to this
	// channel will be blocked if it has already value in it.
	ch := make(chan types.Airport)

	go func() {
		rows, err := conn.Query(`select a.Code,b.Code from dbo.Inf_AirportSTD a
				join dbo.Inf_AirportSTD b on a.CityCode != b.CityCode`)
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		//count := 0
		var airport types.Airport
		for rows.Next() {
			err := rows.Scan(&airport.DepCode, &airport.ArrCode)
			if err != nil {
				log.Fatal(err)
			}

			// this will blocked until it's value have been taken by others.
			ch <- airport
			//count++

			//if count > 1000 {
			//break
			//}
		}
		close(ch)
	}()

	return ch, nil
}

func AirportRequestFilter(airports chan types.Airport) chan types.Request {

	// this channel is non-buffer channel, which means that send to this
	// channel will be blocked if it has already value in it.
	requests := make(chan types.Request)

	go func() {
		for airport := range airports {
			//date := "2018-09-09"
			// note: because date is tomorrow, so this program must not run
			// cross day. e.g. not running this program after 23:00
			date := time.Now().Add(24 * time.Hour).Format("2006-01-02")
			url := fmt.Sprintf("http://www.umetrip.com/mskyweb/fs/fa.do?dep=%s&arr=%s&date=%s",
				airport.DepCode, airport.ArrCode, date)

			requests <- types.Request{
				RawParam: types.Param{
					Dep:  airport.DepCode,
					Arr:  airport.ArrCode,
					Date: date,
				},
				Url:        url,
				ParserFunc: parser.ParseList,
			}
		}

		// can not close this channel, because this channel is used for scheduler's
		// in-channel which failed request (status code is 500) would resend to it
		// to fetch again.
		//close(requests)
	}()

	return requests
}
