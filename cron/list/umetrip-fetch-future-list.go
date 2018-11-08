package main

import (
	"log"

	"time"

	"fmt"

	"os"

	"github.com/champkeh/crawler/fetcher"
	"github.com/champkeh/crawler/persist"
	"github.com/champkeh/crawler/ratelimiter"
	"github.com/champkeh/crawler/scheduler"
	"github.com/champkeh/crawler/source/umetrip"
	"github.com/champkeh/crawler/store"
	"github.com/champkeh/crawler/types"
)

type UmetripListEngine struct {
	RequestScheduler types.RequestScheduler
	RateLimiter      types.RateLimiter
	WorkerCount      int
}

var DefaultUmetripListEngine = UmetripListEngine{
	RequestScheduler: &scheduler.SimpleRequestScheduler{},
	RateLimiter:      ratelimiter.NewSimpleRateLimiterFull(30, 5000, 50),
	WorkerCount:      100,
}

// cron 计划任务
// 每周日开始更新下周的数据
// 0 1 * * 7 umetrip-fetch-future-list
//
// 抓取航班列表（航旅纵横）
// umetrip-fetch-future-list
func main() {
	DefaultUmetripListEngine.Run()
}

func (e UmetripListEngine) Run() {

	// 定义开始与结束时间
	start := time.Now().AddDate(0, 0, 1)
	end := start.AddDate(0, 0, 7)

start:

	// 判断是否已经到达结束时间
	if start.Format("2006-01-02") == end.Format("2006-01-02") {
		//结束
		os.Exit(0)
		return
	}

	// 初始化计数器
	persist.FlightSum = 0
	persist.AirportIndex = 0
	types.T1 = time.Now()

	date := start.Format("2006-01-02")
	// generate airport seed
	airports, err := store.AirportChanForInter()
	if err != nil {
		panic(fmt.Sprintf("store.AirportChanForInter error: %s", err))
	}

	// configure scheduler's in channel
	// this filter will generate tomorrow flight request
	requests := umetrip.ListRequestPipe(airports, date)
	e.RequestScheduler.ConfigureRequestChan(requests)

	// configure scheduler's results channel, has 100 space buffer channel
	results := make(chan types.ParseResult, 1000)

	// create fetch worker
	for i := 0; i < e.WorkerCount; i++ {
		e.CreateWorker(requests, results)
	}

	// run the rate-limiter
	go e.RateLimiter.Run()

	timer := time.NewTimer(3 * time.Minute)
	completed := make(chan bool)
	for {
		timer.Reset(3 * time.Minute)

		// when all result have been handled, this will blocked forever.
		// so, here use `select` to avoid this problem.
		select {
		case result := <-results:
			// 保存数据库
			data, end, err := persist.Save(result, false, e.PrintNotifier, e.RateLimiter)
			if err != nil {
				log.Printf("\nsave %v error: %v\n", data, err)
			}
			if end {
				fmt.Println("\nbegin next date...")
				time.Sleep(5 * time.Second)
				go func() {
					completed <- true
				}()
			}

		case <-timer.C:
			start = start.AddDate(0, 0, 1)
			goto start
		case <-completed:
			start = start.AddDate(0, 0, 1)
			goto start
		}
	}
}

func (e UmetripListEngine) fetchWorker(r types.Request) (types.ParseResult, error) {
	return fetcher.FetchRequest(r, e.RateLimiter)
}

func (e UmetripListEngine) CreateWorker(in chan types.Request, out chan types.ParseResult) {
	go func() {
		for {
			request, ok := <-in
			if ok == false {
				// 结束goroutine
				return
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
