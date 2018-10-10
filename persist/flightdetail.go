package persist

import (
	"strings"

	"fmt"
	"os"

	"database/sql"

	"time"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/datasource/umetrip/parser"
	"github.com/champkeh/crawler/seeds"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/utils"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/labstack/gommon/log"
)

func PrintDetail(result types.ParseResult, notifier types.PrintNotifier,
	limiter types.RateLimiter) {

	var itemCount = 0
	for _, item := range result.Items {
		_ = item.(parser.FlightDetailData)

		itemCount++
	}
	FlightSum++

	data := types.NotifyData{
		Type:        "detail",
		Elapsed:     time.Since(types.T1),
		Date:        result.Request.RawParam.Date,
		FlightCount: itemCount,
		FlightSum:   FlightSum,
		FlightTotal: seeds.TotalFlight,
		Progress:    float32(100 * float64(FlightSum) / float64(seeds.TotalFlight)),
		QPS:         limiter.QPS(),
	}
	notifier.Print(data)

	// task is completed?
	if FlightSum >= seeds.TotalFlight {
		go func() {
			// program exit after 5 seconds
			fmt.Println("Completed! Program will exit after 5 seconds...")
			time.Sleep(5 * time.Second)
			os.Exit(0)
		}()
	}
}

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

// 清除未来详情数据
func ClearDataBase(foreign bool) {
	// 清除之前的数据
	tableprefix := "FutureDetail"
	if foreign {
		// 国际航班对应的表前缀
		tableprefix = "ForeignFutureDetail"
	}
	tabledate := time.Now().Add(24 * time.Hour).Format("200601")

	date := time.Now().Add(24 * time.Hour).Format("2006-01-02")

	query := fmt.Sprintf("delete from [dbo].[%s_%s] where date='%s'", tableprefix, tabledate, date)
	_, err := db.Exec(query)
	if err != nil {
		// note: 有可能表还不存在，所以要忽略这里的错误
		log.Warnf("clear data error: %v", err)
	}
}

// 保存未来详情数据
func SaveDetail(result types.ParseResult, foreign bool, notifier types.PrintNotifier, limiter types.RateLimiter) (
	parser.FlightDetailData, error) {

	tableprefix := "FutureDetail"
	if foreign {
		tableprefix = "ForeignFutureDetail"
	}
	tabledate := strings.Replace(result.Request.RawParam.Date, "-", "", -1)[0:6]

	// 确保数据表存在
	_, err := db.Exec("sp_createFutureDetailTable", sql.Named("tablename", tableprefix+"_"+tabledate))
	if err != nil {
		panic(err)
	}

	var itemCount = 0
	for _, item := range result.Items {
		data := item.(parser.FlightDetailData)

		// 确保不会重复插入
		existCount := 0
		db.QueryRow(fmt.Sprintf(
			"select count(1) from [dbo].[%s_%s] "+
				"where flightNo='%s' and date='%s' and depCode='%s' and arrCode='%s'",
			tableprefix, tabledate,
			data.FlightNo, data.FlightDate, data.DepCode, data.ArrCode)).Scan(&existCount)

		if existCount == 1 {
			// 已经存在了，跳过
			continue
		}

		// 解析起降时间
		depPlanTime := utils.Code2Time(data.DepPlanTime)
		depActualTime := utils.Code2Time(data.DepActualTime)
		arrPlanTime := utils.Code2Time(data.ArrPlanTime)
		arrActualTime := utils.Code2Time(data.ArrActualTime)

		_, err := db.Exec(fmt.Sprintf("insert into [dbo].[%s_%s]"+
			"(flightNo,date,depCode,arrCode,depCity,arrCity,flightState,"+
			"depPlanTime,depActualTime,arrPlanTime,arrActualTime,"+
			"code1,code2,code3,code4,"+
			"mileage,duration,age,"+
			"preFlightNo,preFlightState,preFlightDepCode,preFlightArrCode,"+
			"depWeather,arrWeather,"+
			"depFlow,arrFlow,createAt)"+
			" values"+
			"('%s','%s','%s','%s','%s','%s','%s',"+
			"'%s','%s','%s','%s',"+
			"'%s','%s','%s','%s',"+
			"'%s','%s','%s',"+
			"'%s','%s','%s','%s',"+
			"'%s','%s',"+
			"'%s','%s','%s')", tableprefix, tabledate,
			data.FlightNo, data.FlightDate, data.DepCode, data.ArrCode, data.DepCity, data.ArrCity, data.FlightState,
			utils.TimeToDatetime(data.FlightDate, depPlanTime, depPlanTime),
			utils.TimeToDatetime(data.FlightDate, depPlanTime, depActualTime),
			utils.TimeToDatetime(data.FlightDate, depPlanTime, arrPlanTime),
			utils.TimeToDatetime(data.FlightDate, depPlanTime, arrActualTime),
			data.DepPlanTime,
			data.DepActualTime,
			data.ArrPlanTime,
			data.ArrActualTime,
			data.Mileage, data.Duration, data.Age,
			data.PreFlightNo, data.PreFlightState, data.PreFlightDepCode, data.PreFlightArrCode,
			data.DepWeather, data.ArrWeather, data.DepFlow, data.ArrFlow,
			time.Now().Format("2006-01-02 15:04:05")))
		if err != nil {
			return data, err
		}

		itemCount++
	}
	FlightSum++

	data := types.NotifyData{
		Type:        "detail",
		Elapsed:     time.Since(types.T1),
		Date:        result.Request.RawParam.Date,
		FlightCount: itemCount,
		FlightSum:   FlightSum,
		FlightTotal: seeds.TotalFlight,
		Progress:    float32(100 * float64(FlightSum) / float64(seeds.TotalFlight)),
		QPS:         limiter.QPS(),
	}
	notifier.Print(data)

	// task is completed?
	if FlightSum >= seeds.TotalFlight {
		go func() {
			// program exit after 5 seconds
			fmt.Println("Completed! Program will exit after 5 seconds...")
			time.Sleep(5 * time.Second)
			os.Exit(0)
		}()
	}

	return parser.FlightDetailData{}, nil
}
