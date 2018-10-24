package main

import (
	"database/sql"
	"fmt"

	"time"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/cron/types"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/labstack/gommon/log"
)

const (
	layout = "2006-01-02 15:04:05"
)

// cron 计划任务
// 0 17 * * * copy-init-data-to-realtime
//
// 从FutureDetail表拷贝第2天的实时航班数据到RealTime表中
// copy-init-data-to-realtime
func main() {

	// 打开数据库连接
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s", config.SqlUser, config.SqlPass, config.SqlHost,
		"FlightData")
	db, err := sql.Open("sqlserver", connstr)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	var tablename = time.Now().AddDate(0, 0, 1).Format("200601")
	var date = time.Now().AddDate(0, 0, 1).Format("2006-01-02")

	var query = fmt.Sprintf("select flightNo,date,depCode,arrCode,depCity,arrCity,flightState,"+
		"depPlanTime,depActualTime,arrPlanTime,arrActualTime,"+
		"mileage,duration,age,"+
		"preFlightNo,preFlightState,preFlightDepCode,preFlightArrCode,"+
		"depWeather,arrWeather,"+
		"depFlow,arrFlow from [dbo].[FutureDetail_%s] "+
		"where date='%s'", tablename, date)
	rows, err := db.Query(query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	copyCount := 0
	var model types.RealTimeTableModel
	for rows.Next() {
		err = rows.Scan(
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
			&model.ArrFlow)
		if err != nil {
			fmt.Printf("[%s]: scan FutureDetail error: %s\n", time.Now().Format(layout), err)
			continue
		}

		// 检查 RealTime 表是否存在该航班
		existCount := 0
		err := db.QueryRow(fmt.Sprintf("select count(1) from [dbo].[RealTime] where flightNo='%s' and date='%s' "+
			"and depCode='%s' and arrCode='%s'", model.FlightNo, model.Date, model.DepCode, model.ArrCode)).Scan(&existCount)
		if err != nil {
			panic(err)
		} else if existCount == 0 {
			//写入实时表
			query := fmt.Sprintf("insert into [dbo].[RealTime]"+
				"(flightNo,date,depCode,arrCode,depCity,arrCity,flightState,"+
				"depPlanTime,depExpTime,depActualTime,arrPlanTime,arrExpTime,arrActualTime,"+
				"mileage,duration,age,"+
				"preFlightNo,preFlightState,preFlightDepCode,preFlightArrCode,"+
				"depWeather,arrWeather,depFlow,arrFlow,"+
				"updateAt,source)"+
				" values ("+
				"'%s','%s','%s','%s','%s','%s','%s',"+
				"'%s','%s','%s','%s','%s','%s',"+
				"'%s','%s','%s',"+
				"'%s','%s','%s','%s',"+
				"'%s','%s','%s','%s',"+
				"'%s','%s'"+
				")",
				model.FlightNo,
				model.Date,
				model.DepCode,
				model.ArrCode,
				model.DepCity,
				model.ArrCity,
				model.FlightState,
				model.DepPlanTime,
				model.DepPlanTime,
				model.DepActualTime,
				model.ArrPlanTime,
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
				time.Now().Format("2006-01-02 15:04:05"),
				"umetrip")
			// todo: source should come from detail table
			_, err = db.Exec(query)
			if err != nil {
				log.Printf("insert error: %v", err)
				continue
			}
			copyCount++
		} else {
			log.Printf("entry [%s:%s:%s:%s] exist: %d",
				model.Date, model.FlightNo, model.DepCode, model.ArrCode,
				existCount)
		}
	}

	fmt.Printf("[%s]: success copy %d init data (%s) to realtime\n", time.Now().Format("2006-01-02 15:04:05"),
		copyCount, date)
}
