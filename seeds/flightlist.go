package seeds

import (
	"database/sql"
	"log"

	"fmt"

	"strings"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/umetrip/parser"
	_ "github.com/denisenkom/go-mssqldb"
)

type Flight struct {
	FlightNo   string
	FlightDate string
}

var (
	// 交叉连接之后的航线总数
	TotalFlight int
)

// date format: 2018-09-10
func PullFlightListAt(date string) (chan Flight, error) {

	// connect sql server
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s&connection+timeout=60",
		config.SqlUser, config.SqlPass, config.SqlAddr, "FlightData")
	db, err := sql.Open("sqlserver", connstr)
	if err != nil {
		return nil, err
	}

	// query total flight to fetch
	row := db.QueryRow(fmt.Sprintf("select count(*) from dbo.FutureList_%s where date='%s'",
		strings.Replace(date, "-", "", -1)[0:6], date))
	err = row.Scan(&TotalFlight)
	if err != nil {
		return nil, err
	}

	// this channel is non-buffer channel, which means that send to this
	// channel will be blocked if it has already value in it.
	ch := make(chan Flight)

	go func() {
		query := fmt.Sprintf("select distinct date,flightNo from dbo.FutureList_%s where date='%s'",
			strings.Replace(date, "-", "", -1)[0:6], date)
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
				continue
			}

			// this will blocked until it's value have been taken by others.
			ch <- flight
		}
		close(ch)
	}()

	return ch, nil
}

func FlightRequestFilter(flights chan Flight) chan types.Request {

	// because this channel is used for scheduler's in-channel, which will be snatched
	// by 100 workers (goroutine), so set 100 buffer space is better.
	requests := make(chan types.Request, 100)

	go func() {
		for flight := range flights {
			// 详情页url:http://www.umetrip.com/mskyweb/fs/fc.do?flightNo=MU3924&date=2018-09-13
			url := fmt.Sprintf("http://www.umetrip.com/mskyweb/fs/fc.do?flightNo=%s&date=%s",
				flight.FlightNo, flight.FlightDate)

			requests <- types.Request{
				Url:        url,
				ParserFunc: parser.ParseDetail,
				RawParam: types.Param{
					Date: flight.FlightDate,
					Fno:  flight.FlightNo,
				},
			}
		}
	}()

	return requests
}
