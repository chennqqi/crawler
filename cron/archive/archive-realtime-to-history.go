package main

import (
	"database/sql"
	"fmt"

	"time"

	"strings"

	"github.com/champkeh/crawler/config"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/labstack/gommon/log"
)

type RealTimeTableModel struct {
	ID               int
	FlightNo         string
	Date             string
	DepCode          string
	ArrCode          string
	DepCity          string
	ArrCity          string
	FlightState      string
	DepPlanTime      string
	DepExpTime       string
	DepActualTime    string
	ArrPlanTime      string
	ArrExpTime       string
	ArrActualTime    string
	Mileage          string
	Duration         string
	Age              string
	PreFlightNo      string
	PreFlightState   string
	PreFlightDepCode string
	PreFlightArrCode string
	DepWeather       string
	ArrWeather       string
	DepFlow          string
	ArrFlow          string
	CheckinCounter   string
	BoardGate        string
	BaggageTurntable string
}

// cron 计划任务
// 0 * * * * archive-realtime-to-history
//
// 用来对RealTime表中的航班数据进行归档
// archive-realtime-to-history
func main() {
	// 连接到 FlightData 数据库
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s", config.SqlUser, config.SqlPass, config.SqlHost,
		"FlightData")
	db, err := sql.Open("sqlserver", connstr)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// 对昨天以前已完结的航班进行归档
	date := time.Now().Add(-24 * time.Hour).Format("2006-01-02")
	query := fmt.Sprintf("select id,flightNo,date,depCode,arrCode,depCity,arrCity,flightState,"+
		"depPlanTime,depActualTime,arrPlanTime,arrActualTime,"+
		"mileage,duration,age,"+
		"preFlightNo,preFlightState,preFlightDepCode,preFlightArrCode,"+
		"depWeather,arrWeather,"+
		"depFlow,arrFlow,"+
		"checkinCounter,boardGate,baggageTurntable "+
		"from [dbo].[RealTime] "+
		"where date<='%s' and flightState in ('到达','取消','备降','返航')", date)
	rows, err := db.Query(query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	count := 0
	id := 0
	var model RealTimeTableModel
	for rows.Next() {
		err := rows.Scan(
			&id,
			&model.FlightNo,
			&model.Date,
			&model.DepCode,
			&model.ArrCode,
			&model.DepCity,
			&model.ArrCity,
			&model.FlightState,
			&model.DepPlanTime,
			&model.DepActualTime,
			&model.ArrPlanTime,
			&model.ArrActualTime,
			&model.Mileage,
			&model.Duration,
			&model.Age,
			&model.PreFlightNo,
			&model.PreFlightState,
			&model.PreFlightDepCode,
			&model.PreFlightArrCode,
			&model.DepWeather,
			&model.ArrWeather,
			&model.DepFlow,
			&model.ArrFlow,
			&model.CheckinCounter,
			&model.BoardGate,
			&model.BaggageTurntable,
		)
		if err != nil {
			log.Printf("scan error: %v", err)
			continue
		}

		// 确保history表存在
		tabledate := strings.Replace(model.Date, "-", "", -1)[0:6]
		_, err = db.Exec("sp_createHistoryTable", sql.Named("tablename", "History_"+tabledate))
		if err != nil {
			panic(err)
		}

		// 保存到历史表中
		_, err = db.Exec(fmt.Sprintf("insert into [dbo].[History_"+tabledate+"]"+
			"(flightNo,date,depCode,arrCode,depCity,arrCity,flightState,"+
			"depPlanTime,depActualTime,arrPlanTime,arrActualTime,"+
			"mileage,duration,age,"+
			"preFlightNo,preFlightState,preFlightDepCode,preFlightArrCode,"+
			"depWeather,arrWeather,"+
			"depFlow,arrFlow,"+
			"checkinCounter,boardGate,baggageTurntable,"+
			"createAt) "+
			"values ("+
			"'%s','%s','%s','%s','%s','%s','%s',"+
			"'%s','%s','%s','%s',"+
			"'%s','%s','%s',"+
			"'%s','%s','%s','%s',"+
			"'%s','%s',"+
			"'%s','%s',"+
			"'%s','%s','%s',"+
			"'%s')",
			model.FlightNo,
			model.Date,
			model.DepCode,
			model.ArrCode,
			model.DepCity,
			model.ArrCity,
			model.FlightState,
			model.DepPlanTime,
			model.DepActualTime,
			model.ArrPlanTime,
			model.ArrActualTime,
			model.Mileage,
			model.Duration,
			model.Age,
			model.PreFlightNo,
			model.PreFlightState,
			model.PreFlightDepCode,
			model.PreFlightArrCode,
			model.DepWeather,
			model.ArrWeather,
			model.DepFlow,
			model.ArrFlow,
			model.CheckinCounter,
			model.BoardGate,
			model.BaggageTurntable,
			time.Now().Format("2006-01-02 15:04:05")))
		if err != nil {
			log.Printf("insert error: %v", err)
			continue
		}
		count++

		// remove origin model
		_, err = db.Exec(fmt.Sprintf("delete from [dbo].[RealTime] where id=%d", id))
		if err != nil {
			log.Warnf("delete error: %v", err)
			continue
		}
	}

	fmt.Printf("[%s]: success archive %d entries to history\n",
		time.Now().Format("2006-01-02 15:04:05"), count)
}
