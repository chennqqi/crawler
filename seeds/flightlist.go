package seeds

import (
	"database/sql"
	"log"

	"fmt"

	"strings"

	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/umetrip/parser"
)

type Flight struct {
	FlightNo   string
	FlightDate string
}

// date format: 2018-09-10
func PullFlightListAt(date string) (chan Flight, error) {
	// connect sql
	db, err := sql.Open("sqlserver",
		"sqlserver://sa:123456@localhost:1433?database=data&connection+timeout=10")
	if err != nil {
		return nil, err
	}

	// this channel is non-buffer channel, which means that send to this
	// channel will be blocked if it has already value in it.
	ch := make(chan Flight)

	go func() {
		query := fmt.Sprintf("select date,flightNo from dbo.Airline_%s where date='%s'",
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
			//http://www.umetrip.com/mskyweb/fs/fc.do?flightNo=MU3924&date=2018-09-13
			url := fmt.Sprintf("http://www.umetrip.com/mskyweb/fs/fc.do?flightNo=%s&date=%s",
				flight.FlightNo, flight.FlightDate)

			requests <- types.Request{
				Url:        url,
				ParserFunc: parser.ParseDetail,
			}
		}

		// can not close this channel, because this channel is used for scheduler's
		// in-channel which the failed request (status code is 500) would resend to
		// it to fetch again.
		//close(requests)
	}()

	return requests
}
