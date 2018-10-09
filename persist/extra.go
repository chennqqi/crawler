package persist

import (
	"database/sql"
	"fmt"

	"log"
	"time"

	"github.com/champkeh/crawler/config"
	_ "github.com/denisenkom/go-mssqldb"
)

func SaveToTask(fno string, fdate string, ftime string) {
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s",
		config.SqlUser, config.SqlPass, config.SqlAddr, "FlightBaseData")
	db, err := sql.Open("sqlserver", connstr)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 确保不会重复插入
	existCount := 0
	db.QueryRow(fmt.Sprintf(
		"select count(1) from [dbo].[TodoSearchFlight] "+
			"where FlightNo='%s' and FlightDate='%s'",
		fno, fdate)).Scan(&existCount)

	if existCount == 1 {
		// 已经存在了，跳过
		return
	}

	_, err = db.Exec(fmt.Sprintf("insert into [dbo].[TodoSearchFlight]"+
		"(FlightNo,FlightDate,FlightTime,DtCreate,IsCompleted)"+
		" values"+
		"('%s','%s','%s','%s','%d')",
		fno, fdate, ftime,
		time.Now().Format("2006-01-02 15:04:05"), 0))
	if err != nil {
		log.Println(err)
	}
}
