package main

import (
	"database/sql"
	"log"

	"fmt"

	"github.com/champkeh/crawler/ocr"
	_ "github.com/denisenkom/go-mssqldb"
)

var (
	db *sql.DB
)

func init() {
	var err error
	db, err = sql.Open("sqlserver",
		"sqlserver://sa:123456@localhost:1433?database=data&connection+timeout=10")
	if err != nil {
		panic(err)
	}
}

func main() {

	rows, err := db.Query(`select distinct arrActualTime from dbo.FutureFlightData_201809
where arrActualTime not in (
select code from dbo.code_to_time
)`)
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
