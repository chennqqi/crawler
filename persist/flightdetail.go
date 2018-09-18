package persist

import (
	"time"

	"strings"

	"fmt"
	"os"

	"database/sql"

	"regexp"

	"github.com/champkeh/crawler/ocr"
	"github.com/champkeh/crawler/seeds"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/umetrip/parser"
	_ "github.com/denisenkom/go-mssqldb"
)

func PrintDetail(result types.ParseResult, notifier types.PrintNotifier,
	limiter types.RateLimiter) {

	// task is completed?
	if airportIndex >= seeds.TotalAirports {
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

		_, err := db.Exec("insert into [dbo].[FutureFlightData_" + date + "]" +
			"(flightNo,date,depCode,arrCode,depCity,arrCity,flightState,depPlanTime,depActualTime,arrPlanTime," +
			"arrActualTime,mileage,duration,age,preFlightNo,preFlightState,preFlightDepCode,preFlightArrCode," +
			"depWeather,arrWeather,depFlow,arrFlow)" +
			" values ('" + data.FlightNo + "', '" + data.FlightDate +
			"', '" + data.DepCode + "', '" + data.ArrCode +
			"', '" + data.DepCity + "', '" + data.ArrCity +
			"', '" + data.FlightState +
			"', '" + data.DepPlanTime + "', '" + data.DepActualTime +
			"', '" + data.ArrPlanTime + "', '" + data.ArrActualTime +
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

	re := regexp.MustCompile(`\d{2}:\d{2}`)
	resolve, err := ocr.Resolve(code)
	if err == nil {
		b := re.MatchString(resolve)
		if b {
			// 查询
		}
	}
}
