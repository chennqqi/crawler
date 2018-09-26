package engine

import (
	"time"

	"fmt"

	"os"

	"github.com/champkeh/crawler/fetcher"
	"github.com/champkeh/crawler/notifier"
	"github.com/champkeh/crawler/persist"
	"github.com/champkeh/crawler/scheduler"
	"github.com/champkeh/crawler/seeds"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/utils"
	"github.com/labstack/gommon/log"
)

func init() {
	exists := utils.Exists("realtime.log")
	if !exists {
		os.Create("realtime.log")
	}
}

// RealTimeEngine
//
// 这个引擎用于爬取实时航班数据
type RealTimeEngine struct {
	Scheduler     types.Scheduler
	PrintNotifier types.PrintNotifier
	RateLimiter   types.RateLimiter
	WorkerCount   int
}

// DefaultRealTimeEngine
//
// 实时引擎的默认配置
var DefaultRealTimeEngine = RealTimeEngine{
	Scheduler: &scheduler.SimpleScheduler{},
	PrintNotifier: &notifier.ConsolePrintNotifier{
		RateLimiter: rateLimiter,
	},
	RateLimiter: rateLimiter,
	WorkerCount: 100,
}

// Run
//
// 启动实时抓取引擎
func (e RealTimeEngine) Run() {

	// 实时抓取请求的容器
	reqChannel := make(chan types.Request, 3000)

	// 从未来航班列表中拉取当天最近2小时起飞的航班，放在 reqChannel 容器中
	// note: 由于数据源的问题，可能会拉取到不在2小时之内的航班
	err := seeds.PullLatestFlight(reqChannel, true)
	if err != nil {
		panic(err)
	}

	go func() {
		// 然后，每隔2小时拉取一次
		ticker := time.NewTicker(100 * time.Minute)
		for {
			select {
			case <-ticker.C:
				// 拉取最近2小时起飞的航班，放在 reqChannel 容器中
				err := seeds.PullLatestFlight(reqChannel, false)
				if err != nil {
					panic(err)
				}
			}
		}
	}()

	e.Scheduler.ConfigureRequestChan(reqChannel)

	// configure scheduler's out channel, has 100 space buffer channel
	out := make(chan types.ParseResult, 100)

	// create fetch worker
	for i := 0; i < e.WorkerCount; i++ {
		e.CreateFetchWorker(reqChannel, out)
	}

	// run the rate-limiter
	go e.RateLimiter.Run()

	for {
		select {
		case result := <-out:
			finished, status := persist.PrintRealTime(result, e.RateLimiter, reqChannel)
			if finished == false {
				go func() {
					// 航班没有结束，5分钟之后，继续跟踪
					if status == "暂无" {
						time.Sleep(30 * time.Minute)
					} else {
						time.Sleep(5 * time.Minute)
					}
					result.Request.FetchCount++
					e.Scheduler.Submit(result.Request)
				}()
			}
		}
	}
}

func (e RealTimeEngine) fetchWorker(r types.Request) (types.ParseResult, error) {
	body, err := fetcher.Fetch(r.Url, e.RateLimiter)
	if err != nil {
		log.Warnf("Fetcher: error fetching url %s: %v", r.Url, err)
		log.Warnf("Current Rate: %d", e.RateLimiter.Rate())
		return types.ParseResult{}, err
	}

	result, err := r.ParserFunc(body)
	if err != nil {
		log.Warnf("parse (%s:%s) error: %v", r.RawParam.Date, r.RawParam.Fno, err)
	}
	result.Request = r

	return result, nil
}

func (e RealTimeEngine) CreateFetchWorker(in chan types.Request, out chan types.ParseResult) {
	go func() {
		for {
			request := <-in

			//
			edge := time.Now().Add(-2 * 24 * time.Hour).Format("2006-01-02")

			if request.RawParam.Date <= edge {
				// 前天的航班不再跟踪
				utils.AppendToFile("realtime.log",
					fmt.Sprintf(">=2:[%s]fetch not success: %s:%s:%d\n",
						time.Now().Format("2006-01-02 15:04:05"),
						request.RawParam.Date, request.RawParam.Fno, request.FetchCount))
				continue
			} else if request.RawParam.Date < time.Now().Format("2006-01-02") {
				utils.AppendToFile("realtime.log",
					fmt.Sprintf("==1:[%s]fetch not success: %s:%s:%d\n",
						time.Now().Format("2006-01-02 15:04:05"),
						request.RawParam.Date, request.RawParam.Fno, request.FetchCount))
			}

			parseResult, err := e.fetchWorker(request)
			if err != nil {
				// fetch request failed, submit this request to scheduler to fetch
				// later again.
				e.Scheduler.Submit(request)

				// slow down the rate-limiter
				e.RateLimiter.Slower()
				continue
			}
			// out-channel has 100 buffer space
			out <- parseResult
		}
	}()
}
