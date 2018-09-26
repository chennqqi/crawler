package main

import (
	"database/sql"
	"fmt"

	"time"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/umetrip/parser"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/labstack/gommon/log"
)

// cron 计划任务
//
// 从FutureDetail表拷贝第2天的实时航班数据到RealTime表中
func main() {
	// 打开数据库连接
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s&connection+timeout=60",
		config.SqlUser, config.SqlPass, config.SqlAddr, "FlightData")
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
		"depFlow,arrFlow from [dbo].[FutureDetail_%s]"+
		" where date='%s'", tablename, date)
	rows, err := db.Query(query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	var data parser.FlightDetailData
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
			log.Warnf("scan error: %v", err)
			continue
		}

		//写入实时表
		_, err = db.Exec("insert into [dbo].[RealTime]" +
			"(flightNo,date,depCode,arrCode,depCity,arrCity,flightState," +
			"depPlanTime,depActualTime,arrPlanTime,arrActualTime," +
			"mileage,duration,age," +
			"preFlightNo,preFlightState,preFlightDepCode,preFlightArrCode," +
			"depWeather,arrWeather,depFlow,arrFlow,createAt)" +
			" values ('" + data.FlightNo + "', '" + data.FlightDate +
			"', '" + data.DepCode + "', '" + data.ArrCode +
			"', '" + data.DepCity + "', '" + data.ArrCity +
			"', '" + data.FlightState +
			"', '" + data.DepPlanTime +
			"', '" + data.DepActualTime +
			"', '" + data.ArrPlanTime +
			"', '" + data.ArrActualTime +
			"', '" + data.Mileage + "', '" + data.Duration + "', '" + data.Age +
			"', '" + data.PreFlightNo + "', '" + data.PreFlightState +
			"', '" + data.PreFlightDepCode +
			"', '" + data.PreFlightArrCode +
			"', '" + data.DepWeather + "', '" + data.ArrWeather +
			"', '" + data.DepFlow + "', '" + data.ArrFlow +
			"', '" + time.Now().Format("2006-01-02 15:04:05") + "')")
		if err != nil {
			log.Warnf("insert error: %v", err)
			continue
		}
	}

	fmt.Printf("%s copy completed\n", date)
}
