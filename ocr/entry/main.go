package main

import (
	"database/sql"
	"log"

	"fmt"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/ocr"
	_ "github.com/denisenkom/go-mssqldb"
)

var (
	db *sql.DB
)

func init() {
	var err error

	// 连接到 FlightData 数据库
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s&connection+timeout=10",
		config.SqlUser, config.SqlPass, config.SqlAddr, "FlightData")
	db, err = sql.Open("sqlserver", connstr)
	if err != nil {
		panic(err)
	}
}

func main() {

	rows, err := db.Query(`select distinct code2 from dbo.RealTime
where code2 not in (
select code from dbo.code_to_time
)
`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	var count int
	var code string
	for rows.Next() {
		err := rows.Scan(&code)
		if err != nil {
			log.Fatal(err)
		}

		s, err := ocr.Resolve(code)
		if err != nil {
			fmt.Printf("resolve %s error: %v\n", code, err)
			continue
		}
		err = save(code, s)
		if err != nil {
			fmt.Printf("save (%s:%s) error: %v\n", code, s, err)
			continue
		}
		count++
		fmt.Printf("#%d save (%s:%s) success\n", count, code, s)
	}
}

func save(code, time string) error {
	_, err := db.Exec("insert into [dbo].[code_to_time](code,time)" +
		" values ('" + code + "', '" + time + "')")
	if err != nil {
		return err
	}
	return nil
}
