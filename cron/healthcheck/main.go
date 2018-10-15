package main

import (
	"database/sql"
	"fmt"

	"time"

	"github.com/champkeh/crawler/config"
	_ "github.com/denisenkom/go-mssqldb"
)

// cron 计划任务
//
// 检查检查
// health-check
func main() {

	// 连接到 FlightData 数据库
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s",
		config.SqlUser, config.SqlPass, config.SqlHost, "FlightData")
	db, err := sql.Open("sqlserver", connstr)
	if err != nil {
		panic(err)
	}

	// 检查第二天的航班详情数据是否存在
	tablename := time.Now().Add(24 * time.Hour).Format("200601")
	date := time.Now().Add(24 * time.Hour).Format("2006-01-02")
	detailCount := 0
	err = db.QueryRow(fmt.Sprintf("select count(1) from [dbo].[FutureDetail_%s]"+
		" where date='%s'", tablename, date)).Scan(&detailCount)
	if err != nil {
		// 查询错误
		Notify(fmt.Sprintf("query error: %s", err))
	} else if detailCount < 20000 {
		// 数据量不足
		Notify(fmt.Sprintf("FutureDetail_%s表中date=%q的数据条数为%d(少于20000条)", tablename, date, detailCount))
	}

	// 检查当天实时表的数据条数
	now := time.Now().Format("2006-01-02")
	realtimeCount := 0
	err = db.QueryRow(fmt.Sprintf("select count(1) from [dbo].[RealTime]"+
		" where date='%s'", now)).Scan(&realtimeCount)
	if err != nil {
		Notify(fmt.Sprintf("query error: %s", err))
	} else if realtimeCount < 10000 {
		Notify(fmt.Sprintf("RealTime表中date=%q的数据条数为%d(小于10000条)", now, realtimeCount))
	}

	// 检查是否有未完成的航班数据
	// 也就是非当天且状态不在 (到达,取消,备降,返航,暂无)
	unfinishedCount := 0
	err = db.QueryRow(fmt.Sprintf("select count(1) from [dbo].[RealTime]"+
		" where date<'%s'"+
		" and flightState not in ('到达','取消','备降','返航','暂无')", now)).Scan(&unfinishedCount)
	if err != nil {
		Notify(fmt.Sprintf("query error: %s", err))
	} else if unfinishedCount > 0 {
		Notify(fmt.Sprintf("RealTime表中date<%q的未完成航班条数为%d(大于0条)", now, unfinishedCount))
	}

	// 检查历史表中的数据是否合理
	tablename2 := time.Now().Add(-1 * 24 * time.Hour).Format("200601")
	yestoday := time.Now().Add(-1 * 24 * time.Hour).Format("2006-01-02")
	historyCount := 0
	err = db.QueryRow(fmt.Sprintf("select count(1) from [dbo].[History_%s]"+
		"where date='%s'", tablename2, yestoday)).Scan(&historyCount)
	if err != nil {
		Notify(fmt.Sprintf("query error: %s", err))
	} else if historyCount < 10000 {
		Notify(fmt.Sprintf("History_%s表中date=%q的历史航班条数为%d(小于10000条)", tablename2, yestoday, historyCount))
	}
}

func Notify(msg string) {
	fmt.Printf("[%s] %s\n", time.Now().Format("2006-01-02 15:04:05"), msg)
}
