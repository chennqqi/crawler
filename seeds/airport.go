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

var (
	// 交叉连接之后的航线总数
	TotalAirports int
)

// PullAirportList pull all airport data e.g. (PEK SHA),(SHA LYA) etc from database
func PullAirportList() (chan types.Airport, error) {

	// connect sql server
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s&connection+timeout=10",
		config.SqlUser, config.SqlPass, config.SqlAddr, config.Database)
	db, err := sql.Open("sqlserver", connstr)
	if err != nil {
		return nil, err
	}

	// query total airports to fetch
	row := db.QueryRow(`select count(*) from dbo.Inf_AirportSTD a
				join dbo.Inf_AirportSTD b on a.CityCode != b.CityCode`)
	err = row.Scan(&TotalAirports)
	if err != nil {
		panic(err)
	}

	// this channel is non-buffer channel, which means that send to this
	// channel will be blocked if it has already value in it.
	ch := make(chan types.Airport)

	go func() {
		rows, err := db.Query(`select a.Code,b.Code from dbo.Inf_AirportSTD a
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

			// this will blocked until it's value have been taken by others.
			ch <- airport
		}
		close(ch)
	}()

	return ch, nil
}

type Config struct {
	Date string `json:"date"`
}

var JsonConfig Config = Config{
	Date: "2018-09-16",
}

func init() {
	// read config file to get date
	//content, err := ioutil.ReadFile("./config.json")
	//if err != nil {
	//	panic(err)
	//}
	//err = json.Unmarshal(content, &JsonConfig)
	//if err != nil {
	//	panic(err)
	//}

	// connect sql server
	//db, err := sql.Open("sqlserver",
	//	"sqlserver://sa:123456@localhost:1433?database=data&connection+timeout=10")
	//if err != nil {
	//	panic(err)
	//}
	//// create table to save result
	//_, err = db.Exec("sp_createTable", sql.Named("tablename",
	//	"Airline_"+strings.Replace(JsonConfig.Date, "-", "", -1)[0:6]))
	//if err != nil {
	//	panic(err)
	//}
}

func AirportRequestFilter(airports chan types.Airport) chan types.Request {

	// because this channel is used for scheduler's in-channel, which will be snatched
	// by 100 workers (goroutine), so set 100 buffer space is better.
	requests := make(chan types.Request, 100)

	go func() {
		var date = JsonConfig.Date
		for airport := range airports {
			//date := "2018-09-09"

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
		// in-channel which the failed request (status code is 500) would resend to
		// it to fetch again.
		//close(requests)
	}()

	return requests
}
