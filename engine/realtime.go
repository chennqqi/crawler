package engine

/*
 * This engine is used to crawling the real-time flight data
 */

import (
	"log"

	"time"

	"github.com/champkeh/crawler/fetcher"
	"github.com/champkeh/crawler/notifier"
	"github.com/champkeh/crawler/persist"
	"github.com/champkeh/crawler/scheduler"
	"github.com/champkeh/crawler/seeds"
	"github.com/champkeh/crawler/types"
)

type RealTimeEngine struct {
	Scheduler     types.Scheduler
	PrintNotifier types.PrintNotifier
	RateLimiter   types.RateLimiter
	WorkerCount   int
}

var DefaultRealTimeEngine = RealTimeEngine{
	Scheduler: &scheduler.SimpleScheduler{},
	PrintNotifier: &notifier.ConsolePrintNotifier{
		RateLimiter: rateLimiter,
	},
	RateLimiter: rateLimiter,
	WorkerCount: 100,
}

// Run is the first step to startup the engine.
// this is used to fetch the first batch flight list data
// only once every day, and save result to database
func (e RealTimeEngine) Run() {
	reqChannel := make(chan types.Request, 3000)

	// 从未来航班列表中拉取当天最近2小时起飞的航班，放在 reqChannel 容器中
	err := seeds.PullLatestFlight(reqChannel)
	if err != nil {
		panic(err)
	}

	// 然后，每隔2小时拉取一次
	go func() {
		ticker := time.NewTicker(2 * time.Hour)
		for {
			select {
			case <-ticker.C:
				// 拉取最近2小时起飞的航班，放在 reqChannel 容器中
				err := seeds.PullLatestFlight(reqChannel)
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

	// run the print-notifier
	go e.PrintNotifier.Run()
	// run the rate-limiter
	go e.RateLimiter.Run()

	for {
		select {
		case result := <-out:
			status := persist.PrintRealTime(result, e.RateLimiter, reqChannel)
			if status == false {
				go func() {
					// 航班没有结束，5分钟之后，继续跟踪
					time.Sleep(5 * time.Minute)
					e.Scheduler.Submit(result.Request)
				}()
			}
		}
	}
}

func (e RealTimeEngine) fetchWorker(r types.Request) (types.ParseResult, error) {
	body, err := fetcher.Fetch(r.Url, e.RateLimiter)
	if err != nil {
		log.Printf("\nFetcher: error fetching url %s: %v\n", r.Url, err)
		log.Printf("Current Rate: %d\n", e.RateLimiter.Rate())
		return types.ParseResult{}, err
	}

	result, err := r.ParserFunc(body)
	if err != nil {
		log.Printf("parse (%s:%s) error: %v\n", r.RawParam.Date, r.RawParam.Fno, err)
	}
	result.Request = r

	return result, nil
}

func (e RealTimeEngine) CreateFetchWorker(in chan types.Request, out chan types.ParseResult) {
	go func() {
		for {
			request := <-in
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
