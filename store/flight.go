package store

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/types"
)

var (
	// 交叉连接之后的航线总数
	TotalFlight int
)

// date format: 2018-09-10
// 从未来航班列表中拉取航班号，并加入到 channel 中返回
func FlightListChanAt(date string, foreign bool) (chan types.FlightInfo, error) {

	// connect sql server
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s", config.SqlUser, config.SqlPass, config.SqlHost,
		"FlightData")
	db, err := sql.Open("sqlserver", connstr)
	if err != nil {
		return nil, err
	}

	// query total flight to fetch
	tableprefix := "FutureList"
	if foreign {
		tableprefix = "ForeignFutureList"
	}
	tabledate := strings.Replace(date, "-", "", -1)[0:6]

	row := db.QueryRow(fmt.Sprintf("select count(1) from "+
		"(select distinct date,flightNo from [dbo].[%s_%s] where date='%s') as temp",
		tableprefix, tabledate, date))
	err = row.Scan(&TotalFlight)
	if err != nil {
		return nil, err
	}

	// this channel is non-buffer channel, which means that send to this
	// channel will be blocked if it has already value in it.
	ch := make(chan types.FlightInfo)

	go func() {
		query := fmt.Sprintf("select distinct date,flightNo from [dbo].[%s_%s] where date='%s'",
			tableprefix, tabledate, date)
		rows, err := db.Query(query)
		if err != nil {
			panic(err)
		}
		defer rows.Close()
		defer db.Close()

		var flight types.FlightInfo
		for rows.Next() {
			err := rows.Scan(&flight.FlightDate, &flight.FlightNo)
			if err != nil {
				log.Printf("scan error: %v\n", err)
				continue
			}

			// this will blocked until it's value have been taken by others.
			ch <- flight
		}
		close(ch)
	}()

	return ch, nil
}

// 从 RealTime 表中拉取出计划起飞时间在未来2小时之内的航班信息
// 用于 RealTime-Engine 跑实时数据
func PullLatestFlight(container chan types.FlightInfo, launch bool) error {
	// 打开数据库连接
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s", config.SqlUser, config.SqlPass, config.SqlHost,
		"FlightData")
	db, err := sql.Open("sqlserver", connstr)
	if err != nil {
		return err
	}

	// 计算未来2小时的时间边界范围
	var now = time.Now().Format("2006-01-02 15:04:05")
	var end = time.Now().Add(2 * time.Hour).Format("2006-01-02 15:04:05")

	query := ""
	if launch {
		// 启动时，会加载当天未完成的航班
		query = fmt.Sprintf("select distinct date,flightNo from [dbo].[RealTime] "+
			"where depPlanTime <= '%s' "+
			"and flightState not in ('到达','取消','返航','暂无','提前取消','返航取消','备降取消','返航到达','备降到达')", end)
	} else {
		// 非启动时，仅加载未来2小时的航班
		query = fmt.Sprintf("select distinct date,flightNo from [dbo].[RealTime] "+
			"where depPlanTime >= '%s' "+
			"and depPlanTime <= '%s' "+
			"and flightState not in ('到达','取消','返航','暂无','提前取消','返航取消','备降取消','返航到达','备降到达')", now, end)
	}

	go func() {
		fmt.Printf("\n>>\t#%s# %q\n", time.Now().Format("2006-01-02 15:04:05"), query)
		rows, err := db.Query(query)
		if err != nil {
			panic(err)
		}
		defer rows.Close()
		defer db.Close()

		var flight types.FlightInfo
		for rows.Next() {
			err := rows.Scan(&flight.FlightDate, &flight.FlightNo)
			if err != nil {
				log.Printf("scan error: %v\n", err)
				continue
			}
			container <- flight
		}
	}()

	return nil
}

// 从 RealTime 表中拉取出30分钟之内没有更新的航班信息
// 该数据会直接使用飞常准进行爬取，主要目的是避免出现长时间未进行爬取的遗漏数据
func PullDeadFlight(container chan types.FlightInfo) error {
	// 打开数据库连接
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s", config.SqlUser, config.SqlPass, config.SqlHost,
		"FlightData")
	db, err := sql.Open("sqlserver", connstr)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("select top 2000 date,flightNo from (select distinct date,flightNo from [dbo].[RealTime] " +
		"where source='umetrip' and (checkinCounter='' or boardGate='' or baggageTurntable='')) as temp")

	go func() {
		fmt.Printf("\n>>\t#%s# %q\n", time.Now().Format("2006-01-02 15:04:05"), query)
		rows, err := db.Query(query)
		if err != nil {
			panic(err)
		}
		defer rows.Close()
		defer db.Close()

		var flight types.FlightInfo
		for rows.Next() {
			err := rows.Scan(&flight.FlightDate, &flight.FlightNo)
			if err != nil {
				log.Printf("scan error: %v\n", err)
				continue
			}
			container <- flight
		}
	}()

	return nil
}

// RemoveFlight用来把航班状态更新为“暂无”
// 也就不用再爬取该航班了
func RemoveFlight(flight types.FlightInfo) error {
	// 打开数据库连接
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s", config.SqlUser, config.SqlPass, config.SqlHost,
		"FlightData")
	db, err := sql.Open("sqlserver", connstr)
	if err != nil {
		return err
	}

	query := fmt.Sprintf("update [dbo].[RealTime] set flightState='暂无' where flightNo='%s' and date='%s'",
		flight.FlightNo, flight.FlightDate)
	_, err = db.Exec(query)
	return err
}
