package engine

import (
	"fmt"

	"time"

	"sync"

	"database/sql"
	"strings"

	"log"

	"encoding/json"
	"io/ioutil"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/datasource/ctrip/parser"
	"github.com/champkeh/crawler/ratelimiter"
	"github.com/champkeh/crawler/store"
	"github.com/champkeh/crawler/types"
	_ "github.com/denisenkom/go-mssqldb"
)

// CtripListEngine 用来爬取携程的国内航班列表的引擎
type CtripListEngine struct {
	RateLimiter types.RateLimiter
	WorkerCount int
}

// DeFaultCtripListEngine CtripListEngine默认配置
var DefaultCtripListEngine = CtripListEngine{
	RateLimiter: ratelimiter.NewSimpleRateLimiter(100),
	WorkerCount: 100,
}

type DateConfig struct {
	StartDate string `json:"start"`
	EndDate   string `json:"end"`
}

var db *sql.DB

func init() {
	var err error

	// 连接到 FlightData 数据库
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s", config.SqlUser, config.SqlPass, config.SqlHost,
		"FlightData")
	db, err = sql.Open("sqlserver", connstr)
	if err != nil {
		panic(err)
	}
}

// Run 启动列表爬取引擎
func (e CtripListEngine) Run() {

	// 读取配置文件中的日期
	contents, err := ioutil.ReadFile("./dateConfig.json")
	if err != nil {
		panic(fmt.Sprintf("read dateConfig file error: %s", err))
	}
	var dateConfig DateConfig
	err = json.Unmarshal(contents, &dateConfig)
	if err != nil {
		panic(fmt.Sprintf("parse dateConfig.json error: %s", err))
	}

	// 日期固定为当天，
	date := dateConfig.StartDate

	airports, err := store.CityAirportChanForInter()
	if err != nil {
		panic(fmt.Sprintf("store.CityAirportChanForInter error: %s", err))
	}
	out := make(chan types.ParseResult, 1000)

	for i := 0; i < e.WorkerCount; i++ {
		e.CreateWorker(airports, date, out)
	}

	// 表名前缀，默认为国内表
	tableprefix := "FutureList"
	tabledate := strings.Replace(date, "-", "", -1)[0:6]

	timer := time.NewTicker(10 * time.Millisecond)
	for {
		select {
		case result := <-out:
			for _, item := range result.Items {
				data := item.(parser.FlightListData)

				// 确保不会重复
				existCount := 0
				db.QueryRow(fmt.Sprintf("select count(1) from [dbo].[%s_%s] "+
					"where dep='%s' and arr='%s' and date='%s' and flightNo='%s'",
					tableprefix, tabledate,
					data.DAirportCode, data.AAirportCode, result.Request.RawParam.Date,
					data.FlightNo)).Scan(&existCount)

				if existCount == 0 {
					// 插入新的记录
					_, err = db.Exec(fmt.Sprintf("insert into [dbo].[%s_%s]"+
						"(dep,arr,date,"+
						"depCity,arrCity,depAirport,arrAirport,"+
						"flightNo,flightName,flightState,"+
						"depPlanTime,arrPlanTime,"+
						"depPort,arrPort,"+
						"source,createAt)"+
						" values"+
						"('%s','%s','%s',"+
						"'%s','%s','%s','%s',"+
						"'%s','%s','%s',"+
						"'%s','%s',"+
						"'%s','%s',"+
						"'%s','%s')",
						tableprefix, tabledate,
						data.DAirportCode,
						data.AAirportCode,
						result.Request.RawParam.Date,
						data.DCityName, data.ACityName, data.DAirportName, data.AAirportName,
						data.FlightNo, data.CompanyShortName, data.Status,
						data.PlanDDateTime, data.PlanADateTime,
						data.DTerminal, data.ATerminal,
						"ctrip", time.Now().Format("2006-01-02 15:04:05")))
					if err != nil {
						panic(err)
					}
				} else if existCount == 1 {
					// 已经存在 更新字段
					_, err = db.Exec(fmt.Sprintf("update [dbo].[%s_%s] "+
						"set "+
						"depCity='%s',arrCity='%s',"+
						"depAirport='%s',arrAirport='%s',"+
						"createAt='%s' "+
						"where dep='%s' and arr='%s' and date='%s' and flightNo='%s'",
						tableprefix, tabledate,
						data.DCityName, data.ACityName,
						data.DAirportName, data.AAirportName,
						time.Now().Format("2006-01-02 15:04:05"),
						data.DAirportCode,
						data.AAirportCode,
						result.Request.RawParam.Date,
						data.FlightNo))
					if err != nil {
						panic(err)
					}
				} else {
					log.Printf("[%s:%s %s->%s] exist %d entry\n",
						result.Request.RawParam.Date,
						data.FlightNo,
						data.DAirportCode,
						data.AAirportCode,
						existCount)
				}
			}
		case <-timer.C:
			fmt.Printf("\r[%s] airport: %d/%d",
				time.Since(types.T1), CtripListFetchCount.Count(), store.TotalAirports)
		}
	}
}

type FetchCount struct {
	count int
	mutex sync.Mutex
}

var CtripListFetchCount FetchCount

func (a *FetchCount) Increment() {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	a.count++
}
func (a *FetchCount) Count() int {
	a.mutex.Lock()
	defer a.mutex.Unlock()

	return a.count
}

func (e CtripListEngine) CreateWorker(in chan types.Airport, date string, out chan types.ParseResult) {
	go func() {
		for {
			airport, ok := <-in
			if ok == false {
				// 结束goroutine
				return
			}
			key, err := parser.GetSearchKey(airport.DepCode, airport.ArrCode, date, e.RateLimiter)
			if err != nil {
				go func() {
					// 回炉重练
					in <- airport
				}()
				continue
			}

			result, err := parser.GetListResult(airport.DepCode, airport.ArrCode, date, key, e.RateLimiter)
			if err != nil {
				log.Printf("%s: %s->%s get list result fail: %s", date, airport.DepCode, airport.ArrCode, err)
				go func() {
					// 回炉重练
					in <- airport
				}()
				continue
			}

			CtripListFetchCount.Increment()

			out <- result
		}
	}()
}
