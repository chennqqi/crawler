package persist

import (
	"strings"

	"fmt"
	"os"

	"database/sql"

	"regexp"

	"log"

	"time"

	"github.com/champkeh/crawler/ocr"
	"github.com/champkeh/crawler/seeds"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/umetrip/parser"
	_ "github.com/denisenkom/go-mssqldb"
)

func PrintDetail(result types.ParseResult, notifier types.PrintNotifier,
	limiter types.RateLimiter) {

	var itemCount = 0
	for _, item := range result.Items {
		_ = item.(parser.FlightDetailData)

		itemCount++
	}
	flightSum++

	data := types.NotifyData{
		Type:        "detail",
		Elapsed:     time.Since(types.T1),
		FlightCount: itemCount,
		FlightSum:   flightSum,
		FlightTotal: seeds.TotalFlight,
		Progress:    float32(100 * float64(flightSum) / float64(seeds.TotalFlight)),
		QPS:         limiter.QPS(),
	}
	notifier.Print(data)

	// task is completed?
	if flightSum >= seeds.TotalFlight {
		go func() {
			// program exit after 5 seconds
			fmt.Println("Completed! Program will exit after 5 seconds...")
			time.Sleep(5 * time.Second)
			os.Exit(0)
		}()
	}
}

func SaveDetail(result types.ParseResult, notifier types.PrintNotifier, limiter types.RateLimiter) (
	parser.FlightDetailData, error) {

	//create table to save result
	date := strings.Replace(result.RawParam.Date, "-", "", -1)[0:6]
	_, err := db.Exec("sp_createFutureTable", sql.Named("tablename", "FutureFlightData_"+date))
	if err != nil {
		panic(err)
	}

	var itemCount = 0

	for _, item := range result.Items {
		data := item.(parser.FlightDetailData)

		// 解析起降时间
		depPlanTime := parseTimeCode(data.DepPlanTime)
		depActualTime := parseTimeCode(data.DepActualTime)
		arrPlanTime := parseTimeCode(data.ArrPlanTime)
		arrActualTime := parseTimeCode(data.ArrActualTime)

		_, err := db.Exec("insert into [dbo].[FutureFlightData_" + date + "]" +
			"(flightNo,date,depCode,arrCode,depCity,arrCity,flightState,depPlanTime,depActualTime,arrPlanTime," +
			"arrActualTime,mileage,duration,age,preFlightNo,preFlightState,preFlightDepCode,preFlightArrCode," +
			"depWeather,arrWeather,depFlow,arrFlow)" +
			" values ('" + data.FlightNo + "', '" + data.FlightDate +
			"', '" + data.DepCode + "', '" + data.ArrCode +
			"', '" + data.DepCity + "', '" + data.ArrCity +
			"', '" + data.FlightState +
			"', '" + timeToDatetime(data.FlightDate, depPlanTime, depPlanTime) +
			"', '" + timeToDatetime(data.FlightDate, depPlanTime, depActualTime) +
			"', '" + timeToDatetime(data.FlightDate, depPlanTime, arrPlanTime) +
			"', '" + timeToDatetime(data.FlightDate, depPlanTime, arrActualTime) +
			"', '" + data.Mileage + "', '" + data.Duration + "', '" + data.Age +
			"', '" + data.PreFlightNo + "', '" + data.PreFlightState +
			"', '" + data.PreFlightDepCode +
			"', '" + data.PreFlightArrCode +
			"', '" + data.DepWeather + "', '" + data.ArrWeather +
			"', '" + data.DepFlow + "', '" + data.ArrFlow + "')")
		if err != nil {
			return data, err
		}

		itemCount++

	}
	flightSum++

	data := types.NotifyData{
		Type:        "detail",
		Elapsed:     time.Since(types.T1),
		FlightCount: itemCount,
		FlightSum:   flightSum,
		FlightTotal: seeds.TotalFlight,
		Progress:    float32(100 * float64(flightSum) / float64(seeds.TotalFlight)),
		QPS:         limiter.QPS(),
	}
	notifier.Print(data)

	// task is completed?
	if flightSum >= seeds.TotalFlight {
		go func() {
			// program exit after 5 seconds
			fmt.Println("Completed! Program will exit after 5 seconds...")
			time.Sleep(5 * time.Second)
			os.Exit(0)
		}()
	}

	return parser.FlightDetailData{}, nil
}

func parseTimeCode(code string) string {
	// 查数据库
	s, err := ocr.CodeToTime(code)
	if err == nil {
		// 数据库命中
		return s
	}

	// 说明数据库不存在该code
	re := regexp.MustCompile(`[0-9\-]{2}:[0-9\-]{2}`)
	resolve, err := ocr.Resolve(code)
	if err == nil {
		// 解析成功
		b := re.MatchString(resolve)
		if b {
			// 写入到数据库
			go func() {
				_, err := db.Exec("insert into dbo.code_to_time(code,time) values('" + code + "','" + resolve + "')")
				if err != nil {
					log.Printf("save resolve result(%s:%s) to database fail: %v", code, resolve, err)
				}
			}()

			return resolve
		}
	}

	// 解析失败
	log.Printf("resolve %s failed: %v\n", code, err)
	return ""
}

func timeToDatetime(date, deptime, arrtime string) string {
	if deptime == "" || arrtime == "" {
		return "1990-01-01 00:00"
	}

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
