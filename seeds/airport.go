package seeds

import (
	"database/sql"

	"log"

	"fmt"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/datasource/umetrip/parser"
	"github.com/champkeh/crawler/types"
	_ "github.com/denisenkom/go-mssqldb"
)

var (
	// 交叉连接之后的航线总数，也就是请求总数
	TotalAirports int
)

// 拉取国内航班的机场三字码组合
func PullAirportList() (chan types.Airport, error) {

	// 从基础数据库中查所有机场三字码组合
	db, err := sql.Open("sqlserver", fmt.Sprintf("sqlserver://%s:%s@%s?database=%s",
		config.SqlUser, config.SqlPass, config.SqlAddr, "FlightBaseData"))
	if err != nil {
		return nil, err
	}

	// query total airports to fetch
	row := db.QueryRow(`select count(1) from dbo.Inf_AirportSTD a
				join dbo.Inf_AirportSTD b on a.CityCode != b.CityCode`)
	err = row.Scan(&TotalAirports)
	if err != nil {
		return nil, err
	}

	// this channel is non-buffer channel, which means that send to this
	// channel will be blocked if it has already value in it.
	ch := make(chan types.Airport)

	go func() {
		rows, err := db.Query(`select distinct a.Code,b.Code from dbo.Inf_AirportSTD a
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

// 拉取国际航班的机场三字码组合
func PullForeignAirportList() (chan types.Airport, error) {
	// 从基础数据库中查所有机场三字码组合
	db, err := sql.Open("sqlserver", fmt.Sprintf("sqlserver://%s:%s@%s?database=%s",
		config.SqlUser, config.SqlPass, config.SqlAddr, "FlightBaseData"))
	if err != nil {
		return nil, err
	}

	// query total airports to fetch
	row := db.QueryRow(`select count(1) from [dbo].[Inf_AirportUME] a
join dbo.Inf_AirportUME b on a.CityCode != b.CityCode
where a.Country != 'china' and b.Country != 'china'`)
	err = row.Scan(&TotalAirports)
	if err != nil {
		return nil, err
	}

	// this channel is non-buffer channel, which means that send to this
	// channel will be blocked if it has already value in it.
	ch := make(chan types.Airport)

	go func() {
		rows, err := db.Query(`select distinct a.tcode,b.tcode from [dbo].[Inf_AirportUME] a
join dbo.Inf_AirportUME b on a.CityCode != b.CityCode
where a.Country != 'china' and b.Country != 'china'`)
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

// date format: "2018-09-09"
// 将机场三字码添加日期属性，构成 request
func AirportRequestFilter(airports chan types.Airport, date string) chan types.Request {

	// because this channel is used for scheduler's in-channel, which will be snatched
	// by 100 workers (goroutine), so set 100 buffer space is better.
	requests := make(chan types.Request, 1000)

	go func() {
		for airport := range airports {
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
	}()

	return requests
}
