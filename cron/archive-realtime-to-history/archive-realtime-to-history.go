package main

import (
	"database/sql"
	"fmt"

	"time"

	"strings"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/cron/types"
	_ "github.com/denisenkom/go-mssqldb"
)

const (
	layout = "2006-01-02 15:04:05"
)

// cron 计划任务
// 0 * * * * archive-realtime-to-history
//
// 用来对RealTime表中已结束的航班数据进行归档
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

	// 检索出昨天及更早之前的已结束航班
	date := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
	query := fmt.Sprintf("select "+
		"id,flightNo,date,depCode,arrCode,depCity,arrCity,flightState,"+
		"depPlanTime,depActualTime,arrPlanTime,arrActualTime,"+
		"mileage,duration,age,"+
		"preFlightNo,preFlightState,preFlightDepCode,preFlightArrCode,"+
		"depWeather,arrWeather,"+
		"depFlow,arrFlow,"+
		"checkinCounter,boardGate,baggageTurntable "+
		"from [dbo].[RealTime] "+
		"where date<='%s' and flightState in ('到达','取消','返航','返航取消','备降取消','提前取消','返航到达','备降到达','暂无')", date)
	rows, err := db.Query(query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	count := 0
	var model types.RealTimeTableModel
	for rows.Next() {
		err := rows.Scan(
			&model.ID,
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
			fmt.Printf("[%s]: scan RealTime error: %s\n", time.Now().Format(layout), err)
			continue
		}

		// 确保history表存在
		tabledate := strings.Replace(model.Date, "-", "", -1)[0:6]
		_, err = db.Exec("sp_createHistoryTable", sql.Named("tablename", "History_"+tabledate))
		if err != nil {
			panic(err)
		}

		// 检查是否已存在
		exists := 0
		err = db.QueryRow(fmt.Sprintf("select count(1) from [dbo].[History_%s] where flightNo='%s' and date='%s' and "+
			"depCode='%s' and arrCode='%s'", tabledate, model.FlightNo, model.Date, model.DepCode, model.ArrCode)).Scan(
			&exists)
		if err != nil {
			panic(err)
		}

		if exists > 0 {
			fmt.Printf("[%s]: already exist %s:%s:%s:%s in History\n", time.Now().Format(layout), model.Date,
				model.FlightNo, model.DepCode, model.ArrCode)
			continue
		}

		// 保存到历史表中
		_, err = db.Exec(fmt.Sprintf("insert into [dbo].[History_%s]"+
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
			"'%s')", tabledate,
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
			fmt.Printf("[%s]: insert into History error: %s\n", time.Now().Format(layout), err)
			continue
		}
		count++

		// remove origin model
		_, err = db.Exec(fmt.Sprintf("delete from [dbo].[RealTime] where id=%d", model.ID))
		if err != nil {
			fmt.Printf("[%s]: delete from RealTime error: %s\n", time.Now().Format(layout), err)
			continue
		}
	}

	fmt.Printf("[%s]: archive %d entries to History\n", time.Now().Format(layout), count)
}
