package persist

import (
	"database/sql"

	"time"

	"strings"

	"fmt"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/datasource/umetrip/parser"
	"github.com/champkeh/crawler/seeds"
	"github.com/champkeh/crawler/types"
	_ "github.com/denisenkom/go-mssqldb"
)

var (
	db *sql.DB
)

func init() {
	var err error

	// 连接到 FlightData 数据库
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s",
		config.SqlUser, config.SqlPass, config.SqlAddr, "FlightData")
	db, err = sql.Open("sqlserver", connstr)
	if err != nil {
		panic(err)
	}
}

var AirportIndex = 0
var FlightSum = 0

func Print(result types.ParseResult, notifier types.PrintNotifier,
	limiter types.RateLimiter) bool {

	var itemCount = 0
	for _, item := range result.Items {
		_ = item.(parser.FlightListData)

		itemCount++
		FlightSum++
	}
	AirportIndex++

	data := types.NotifyData{
		Type:    "list",
		Elapsed: time.Since(types.T1),
		Date:    result.Request.RawParam.Date,
		Airport: types.Airport{
			DepCode: result.Request.RawParam.Dep,
			ArrCode: result.Request.RawParam.Arr},
		AirportIndex: AirportIndex,
		AirportTotal: seeds.TotalAirports,
		FlightCount:  itemCount,
		FlightSum:    FlightSum,
		Progress:     float32(100 * float64(AirportIndex) / float64(seeds.TotalAirports)),
		QPS:          limiter.QPS(),
	}
	notifier.Print(data)

	// task is completed?
	if AirportIndex >= seeds.TotalAirports {
		return true
	} else {
		return false
	}
}

func Save(result types.ParseResult, foreign bool, notifier types.PrintNotifier, limiter types.RateLimiter) (
	parser.FlightListData, bool, error) {

	// 表名前缀，默认为国内表
	tableprefix := "FutureList"
	if foreign {
		// 国际航班对应的表前缀
		tableprefix = "ForeignFutureList"
	}
	tabledate := strings.Replace(result.Request.RawParam.Date, "-", "", -1)[0:6]

	_, err := db.Exec("sp_createFutureListTable", sql.Named("tablename", tableprefix+"_"+tabledate))
	if err != nil {
		panic(err)
	}

	var itemCount = 0
	for _, item := range result.Items {
		data := item.(parser.FlightListData)
		split := strings.Split(data.Airport, "/")

		// 确保不会重复
		existCount := 0
		db.QueryRow(fmt.Sprintf(
			"select count(1) from [dbo].[%s_%s] "+
				"where dep='%s' and arr='%s' and date='%s' and flightNo='%s'",
			tableprefix, tabledate,
			result.Request.RawParam.Dep, result.Request.RawParam.Arr, result.Request.RawParam.Date,
			data.FlightNo)).Scan(&existCount)

		if existCount == 1 {
			// 已经存在了，跳过
			continue
		}
		_, err = db.Exec(fmt.Sprintf("insert into [dbo].[%s_%s]"+
			"(dep,arr,date,"+
			"flightNo,flightName,flightState,"+
			"depPlanTime,arrPlanTime,"+
			"depPort,arrPort,source,createAt)"+
			" values"+
			"('%s','%s','%s',"+
			"'%s','%s','%s',"+
			"'%s','%s',"+
			"'%s','%s','%s','%s')", tableprefix, tabledate,
			result.Request.RawParam.Dep,
			result.Request.RawParam.Arr,
			result.Request.RawParam.Date,
			data.FlightNo, data.FlightCompany, data.State,
			(result.Request.RawParam.Date + " " + data.DepTimePlan),
			fixarrdate(result.Request.RawParam.Date, data.DepTimePlan, data.ArrTimePlan),
			strings.Replace(split[0], "-", "", -1),
			strings.Replace(split[1], "-", "", -1),
			"umetrip",
			time.Now().Format("2006-01-02 15:04:05")))
		if err != nil {
			return data, false, err
		}

		itemCount++
		FlightSum++
	}

	AirportIndex++

	data := types.NotifyData{
		Type:    "list",
		Elapsed: time.Since(types.T1),
		Date:    result.Request.RawParam.Date,
		Airport: types.Airport{
			DepCode: result.Request.RawParam.Dep,
			ArrCode: result.Request.RawParam.Arr},
		AirportIndex: AirportIndex,
		AirportTotal: seeds.TotalAirports,
		FlightCount:  itemCount,
		FlightSum:    FlightSum,
		Progress:     float32(100 * float64(AirportIndex) / float64(seeds.TotalAirports)),
		QPS:          limiter.QPS(),
	}
	notifier.Print(data)

	// task is completed?
	if AirportIndex >= seeds.TotalAirports {
		return parser.FlightListData{}, true, nil
	} else {
		return parser.FlightListData{}, false, nil
	}
}

func fixarrdate(date, deptime, arrtime string) string {
	if arrtime >= deptime {
		return date + " " + arrtime
	} else {
		parse, err := time.Parse("2006-01-02", date)
		if err != nil {
			panic(err)
		}
		return parse.Add(24*time.Hour).Format("2006-01-02") + " " + arrtime
	}
}
