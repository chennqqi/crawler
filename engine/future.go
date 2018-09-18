package engine

import (
	"fmt"
	"log"
	"time"

	"github.com/champkeh/crawler/fetcher"
	"github.com/champkeh/crawler/notifier"
	"github.com/champkeh/crawler/persist"
	"github.com/champkeh/crawler/scheduler"
	"github.com/champkeh/crawler/seeds"
	"github.com/champkeh/crawler/types"
)

// FutureEngine
//
// 这个引擎用来抓取未来航班的详情数据(机型、前序航班信息、...)
// 只需要抓取未来1天的数据即可，因为只有未来1天的航班有前序航班信息
type FutureEngine struct {
	Scheduler     types.Scheduler
	PrintNotifier types.PrintNotifier
	RateLimiter   types.RateLimiter
	WorkerCount   int
}

// DefaultFutureEngine
//
// FutureEngine 的默认实现
var DefaultFutureEngine = FutureEngine{
	Scheduler: &scheduler.SimpleScheduler{},
	PrintNotifier: &notifier.ConsolePrintNotifier{
		RateLimiter: rateLimiter,
	},

	// 采用全局的 rateLimiter
	RateLimiter: rateLimiter,
	WorkerCount: 100,
}

// Run 运行引擎
func (e FutureEngine) Run() {
	// 从未来航班列表中拉取要抓取的航班列表
	// 因为要作为计划任务每天执行，所以日期使用明天
	//var date = time.Now().Add(-10 * 24 * time.Hour).Format("2006-01-02")
	flightlist, err := seeds.PullFlightListAt("2018-09-22")
	if err != nil {
		panic(err)
	}

	// configure scheduler's in channel
	// this filter will generate tomorrow flight request
	reqChannel := seeds.FlightRequestFilter(flightlist)
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

	timer := time.NewTimer(3 * time.Minute)
	for {
		timer.Reset(3 * time.Minute)

		// when all result have been handled, this will blocked forever.
		// so, here use `select` to avoid this problem.
		select {
		case result := <-out:
			//persist.PrintDetail(result, e.PrintNotifier, e.RateLimiter)

			// this is save to database
			go func() {
				data, err := persist.SaveDetail(result, e.PrintNotifier, e.RateLimiter)
				if err != nil {
					log.Printf("\nsave %v error: %v\n", data, err)
				}
			}()

		case <-timer.C:
			fmt.Println("Read timeout, exit the program.")
			return
		}

	}
}

func (e FutureEngine) fetchWorker(r types.Request) (types.ParseResult, error) {
	body, err := fetcher.Fetch(r.Url, e.RateLimiter)
	if err != nil {
		log.Printf("\nFetcher: error fetching url %s: %v\n", r.Url, err)
		log.Printf("Current Rate: %d\n", e.RateLimiter.Rate())
		return types.ParseResult{}, err
	}

	result, err := r.ParserFunc(body)
	if err != nil {
		log.Printf("\n%s:%s 解析失败:%s\n", r.RawParam.Date, r.RawParam.Fno, err)
	}
	result.RawParam = r.RawParam

	return result, nil
}

func (e FutureEngine) CreateFetchWorker(in chan types.Request, out chan types.ParseResult) {
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
