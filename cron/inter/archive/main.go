package main

import (
	"database/sql"
	"fmt"

	"time"

	"strings"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/datasource/umetrip/parser"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/labstack/gommon/log"
)

// cron 计划任务
//
// 用来对RealTime表中的航班数据进行归档
// archive-realtime-to-history
func main() {
	// 连接到 FlightData 数据库
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s",
		config.SqlUser, config.SqlPass, config.SqlAddr, "FlightData")
	db, err := sql.Open("sqlserver", connstr)
	if err != nil {
		panic(err)
	}

	// 对昨天以前已完结的航班进行归档
	date := time.Now().Add(-24 * time.Hour).Format("2006-01-02")
	query := fmt.Sprintf("select id,flightNo,date,depCode,arrCode,depCity,arrCity,flightState,"+
		"depPlanTime,depActualTime,arrPlanTime,arrActualTime,"+
		"mileage,duration,age,"+
		"preFlightNo,preFlightState,preFlightDepCode,preFlightArrCode,"+
		"depWeather,arrWeather,"+
		"depFlow,arrFlow from [dbo].[RealTime] "+
		"where date<='%s' and flightState in ('到达','取消','备降','返航')", date)
	rows, err := db.Query(query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	count := 0
	id := 0
	var flightDetail parser.FlightDetailData
	for rows.Next() {
		err := rows.Scan(
			&id,
			&flightDetail.FlightNo,
			&flightDetail.FlightDate,
			&flightDetail.DepCode,
			&flightDetail.ArrCode,
			&flightDetail.DepCity,
			&flightDetail.ArrCity,
			&flightDetail.FlightState,
			&flightDetail.DepPlanTime,
			&flightDetail.DepActualTime,
			&flightDetail.ArrPlanTime,
			&flightDetail.ArrActualTime,
			&flightDetail.Mileage,
			&flightDetail.Duration,
			&flightDetail.Age,
			&flightDetail.PreFlightNo,
			&flightDetail.PreFlightState,
			&flightDetail.PreFlightDepCode,
			&flightDetail.PreFlightArrCode,
			&flightDetail.DepWeather,
			&flightDetail.ArrWeather,
			&flightDetail.DepFlow,
			&flightDetail.ArrFlow)
		if err != nil {
			log.Warnf("scan error: %v", err)
			continue
		}

		// 确保history表存在
		tabledate := strings.Replace(flightDetail.FlightDate, "-", "", -1)[0:6]
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
			"depFlow,arrFlow,createAt) "+
			"values ("+
			"'%s','%s','%s','%s','%s','%s','%s',"+
			"'%s','%s','%s','%s',"+
			"'%s','%s','%s',"+
			"'%s','%s','%s','%s',"+
			"'%s','%s',"+
			"'%s','%s','%s')",
			flightDetail.FlightNo,
			flightDetail.FlightDate,
			flightDetail.DepCode,
			flightDetail.ArrCode,
			flightDetail.DepCity,
			flightDetail.ArrCity,
			flightDetail.FlightState,
			flightDetail.DepPlanTime,
			flightDetail.DepActualTime,
			flightDetail.ArrPlanTime,
			flightDetail.ArrActualTime,
			flightDetail.Mileage,
			flightDetail.Duration,
			flightDetail.Age,
			flightDetail.PreFlightNo,
			flightDetail.PreFlightState,
			flightDetail.PreFlightDepCode,
			flightDetail.PreFlightArrCode,
			flightDetail.DepWeather,
			flightDetail.ArrWeather,
			flightDetail.DepFlow,
			flightDetail.ArrFlow,
			time.Now().Format("2006-01-02 15:04:05")))
		if err != nil {
			log.Warnf("insert error: %v", err)
			continue
		}
		count++

		// remove origin data
		_, err = db.Exec(fmt.Sprintf("delete from [dbo].[RealTime] where id=%d", id))
		if err != nil {
			log.Warnf("delete error: %v", err)
			continue
		}
	}

	fmt.Printf("[%s] archive %d completed\n",
		time.Now().Format("2006-01-02 15:04:05"), count)
}
