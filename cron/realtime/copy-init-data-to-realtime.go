package main

import (
	"database/sql"
	"fmt"

	"time"

	"github.com/champkeh/crawler/config"
	umetripParser "github.com/champkeh/crawler/datasource/umetrip/parser"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/labstack/gommon/log"
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

	var tablename = time.Now().Add(24 * time.Hour).Format("200601")
	var date = time.Now().Add(24 * time.Hour).Format("2006-01-02")

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
	var data umetripParser.FlightDetailData
	for rows.Next() {
		err = rows.Scan(
			&data.FlightNo,
			&data.FlightDate,
			&data.DepCode,
			&data.ArrCode,
			&data.DepCity,
			&data.ArrCity,
			&data.FlightState,
			&data.DepPlanTime,
			&data.DepActualTime,
			&data.ArrPlanTime,
			&data.ArrActualTime,
			&data.Mileage,
			&data.Duration,
			&data.Age,
			&data.PreFlightNo,
			&data.PreFlightState,
			&data.PreFlightDepCode,
			&data.PreFlightArrCode,
			&data.DepWeather,
			&data.ArrWeather,
			&data.DepFlow,
			&data.ArrFlow)
		if err != nil {
			log.Printf("scan error: %v", err)
			continue
		}

		// 检查是否存在
		existCount := 0
		err := db.QueryRow(fmt.Sprintf("select count(1) from [dbo].[RealTime]"+
			" where flightNo='%s' and date='%s' and depCode='%s' and arrCode='%s'",
			data.FlightNo, data.FlightDate, data.DepCode, data.ArrCode)).Scan(&existCount)
		if err != nil {
			log.Printf("scan error: %v", err)
			continue
		} else if existCount == 0 {
			//写入实时表
			query := fmt.Sprintf("insert into [dbo].[RealTime]"+
				"(flightNo,date,depCode,arrCode,depCity,arrCity,flightState,"+
				"depPlanTime,depExpTime,depActualTime,arrPlanTime,arrExpTime,arrActualTime,"+
				"mileage,duration,age,"+
				"preFlightNo,preFlightState,preFlightDepCode,preFlightArrCode,"+
				"depWeather,arrWeather,depFlow,arrFlow,"+
				"updateAt,source,failcount)"+
				" values ("+
				"'%s','%s','%s','%s','%s','%s','%s',"+
				"'%s','%s','%s','%s','%s','%s',"+
				"'%s','%s','%s',"+
				"'%s','%s','%s','%s',"+
				"'%s','%s','%s','%s',"+
				"'%s','%s','%d'"+
				")",
				data.FlightNo,
				data.FlightDate,
				data.DepCode,
				data.ArrCode,
				data.DepCity,
				data.ArrCity,
				data.FlightState,
				data.DepPlanTime,
				data.DepPlanTime,
				data.DepActualTime,
				data.ArrPlanTime,
				data.ArrPlanTime,
				data.ArrActualTime,
				data.Mileage,
				data.Duration,
				data.Age,
				data.PreFlightNo,
				data.PreFlightState,
				data.PreFlightDepCode,
				data.PreFlightArrCode,
				data.DepWeather,
				data.ArrWeather,
				data.DepFlow,
				data.ArrFlow,
				time.Now().Format("2006-01-02 15:04:05"),
				"umetrip", 0)
			_, err = db.Exec(query)
			if err != nil {
				log.Printf("insert error: %v", err)
				continue
			}
			copyCount++
		} else {
			log.Printf("entry [%s:%s:%s:%s] exist: %d",
				data.FlightDate, data.FlightNo, data.DepCode, data.ArrCode,
				existCount)
		}
	}

	fmt.Printf("[%s]: success copy %d init data (%s) to realtime\n", time.Now().Format("2006-01-02 15:04:05"),
		copyCount, date)
}
