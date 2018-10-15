package main

import (
	"database/sql"
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
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s",
		config.SqlUser, config.SqlPass, config.SqlHost, "FlightData")
	db, err = sql.Open("sqlserver", connstr)
	if err != nil {
		panic(err)
	}
}

func main() {
	code := "JVOwmjsiHgwuGyS1QvWtTQ=="

	s, err := ocr.Resolve(code)
	if err != nil {
		fmt.Printf("resolve %s error: %v\n", code, err)
		return
	}
	err = save(code, s)
	if err != nil {
		fmt.Printf("save (%s:%s) error: %v\n", code, s, err)
		return
	}

	fmt.Printf("save (%s:%s) success\n", code, s)
}

func save(code, time string) error {
	_, err := db.Exec("insert into [dbo].[code_to_time](code,time)" +
		" values ('" + code + "', '" + time + "')")
	if err != nil {
		return err
	}
	return nil
}
