package store

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/types"
	_ "github.com/denisenkom/go-mssqldb"
)

var (
	// 交叉连接之后的航线总数，也就是请求总数
	TotalAirports int
)

// 国内 机场三字码组合
//
// 可用于航旅纵横
func AirportChanForInter() (chan types.Airport, error) {

	// 从基础数据库中查所有机场三字码组合
	db, err := sql.Open("sqlserver", fmt.Sprintf("sqlserver://%s:%s@%s?database=%s",
		config.SqlUser, config.SqlPass, config.SqlHost, "FlightBaseData"))
	if err != nil {
		return nil, err
	}

	// 查询国内机场的所有组合总数
	row := db.QueryRow(`select count(1) from (select distinct a.Code code1, b.Code code2
 from dbo.Inf_AirportSTD a join dbo.Inf_AirportSTD b on a.CityCode != b.CityCode) as tmp`)
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

// 国内 城市三字码组合
//
// 可用于携程
func CityAirportChanForInter() (chan types.Airport, error) {

	// 从基础数据库中查所有机场三字码组合
	db, err := sql.Open("sqlserver", fmt.Sprintf("sqlserver://%s:%s@%s?database=%s",
		config.SqlUser, config.SqlPass, config.SqlHost, "FlightBaseData"))
	if err != nil {
		return nil, err
	}

	// query total airports to fetch
	err = db.QueryRow(`select count(1) from
 (select distinct a.CityCode code1, b.CityCode code2 from dbo.Inf_AirportSTD a
  join dbo.Inf_AirportSTD b on a.CityCode != b.CityCode) as tmp`).Scan(&TotalAirports)
	if err != nil {
		return nil, err
	}

	// this channel is non-buffer channel, which means that send to this
	// channel will be blocked if it has already value in it.
	ch := make(chan types.Airport)

	go func() {
		rows, err := db.Query(`select distinct a.CityCode,b.CityCode from dbo.Inf_AirportSTD a
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
		//close(ch)
	}()

	return ch, nil
}

// 国际 机场三字码组合
//
// 可用于航旅纵横
func AirportChanForForeign() (chan types.Airport, error) {
	// 从基础数据库中查所有机场三字码组合
	db, err := sql.Open("sqlserver", fmt.Sprintf("sqlserver://%s:%s@%s?database=%s",
		config.SqlUser, config.SqlPass, config.SqlHost, "FlightBaseData"))
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

// 国际 城市三字码组合
//
// 可用于携程
func CityAirportChanForForeign() (chan types.Airport, error) {
	// 从基础数据库中查所有机场三字码组合
	db, err := sql.Open("sqlserver", fmt.Sprintf("sqlserver://%s:%s@%s?database=%s",
		config.SqlUser, config.SqlPass, config.SqlHost, "FlightBaseData"))
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
		rows, err := db.Query(`select distinct a.CityCode,b.CityCode from [dbo].[Inf_AirportUME] a
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
