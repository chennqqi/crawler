package seeds

import (
	"database/sql"

	"fmt"

	"strings"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/umetrip/parser"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/labstack/gommon/log"
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
// 从未来航班列表中拉取航班号，并加入到 channel 中返回
func PullFlightListAt(date string, foreign bool) (chan Flight, error) {

	// connect sql server
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s",
		config.SqlUser, config.SqlPass, config.SqlAddr, "FlightData")
	db, err := sql.Open("sqlserver", connstr)
	if err != nil {
		return nil, err
	}

	// query total flight to fetch
	tableprefix := "FutureList"
	if foreign {
		tableprefix = "ForeignFutureList"
	}
	tabledate := strings.Replace(date, "-", "", -1)[0:6]

	row := db.QueryRow(fmt.Sprintf("select count(1) from "+
		"(select distinct date,flightNo from [dbo].[%s_%s] where date='%s') as temp",
		tableprefix, tabledate, date))
	err = row.Scan(&TotalFlight)
	if err != nil {
		return nil, err
	}

	// this channel is non-buffer channel, which means that send to this
	// channel will be blocked if it has already value in it.
	ch := make(chan Flight)

	go func() {
		query := fmt.Sprintf("select distinct date,flightNo from [dbo].[%s_%s] where date='%s'",
			tableprefix, tabledate, date)
		rows, err := db.Query(query)
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		var flight Flight
		for rows.Next() {
			err := rows.Scan(&flight.FlightDate, &flight.FlightNo)
			if err != nil {
				log.Warnf("scan error: %v", err)
				continue
			}

			// this will blocked until it's value have been taken by others.
			ch <- flight
		}
		close(ch)
	}()

	return ch, nil
}

// 把航班号添加日期，构造成 request
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
