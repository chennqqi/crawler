package foreign

import (
	"log"

	"time"

	"fmt"

	"encoding/json"
	"io/ioutil"

	"github.com/champkeh/crawler/fetcher"
	"github.com/champkeh/crawler/notifier"
	"github.com/champkeh/crawler/persist"
	"github.com/champkeh/crawler/ratelimiter"
	"github.com/champkeh/crawler/scheduler"
	"github.com/champkeh/crawler/source/umetrip"
	"github.com/champkeh/crawler/store"
	"github.com/champkeh/crawler/types"
)

// 国外机场列表获取引擎
type SimpleEngine struct {
	Scheduler     types.RequestScheduler
	PrintNotifier types.PrintNotifier
	RateLimiter   types.RateLimiter
	WorkerCount   int
}

var rateLimiter = ratelimiter.NewSimpleRateLimiter(30)

var DefaultSimpleEngine = SimpleEngine{
	Scheduler: &scheduler.SimpleRequestScheduler{},
	PrintNotifier: &notifier.ConsolePrintNotifier{
		RateLimiter: rateLimiter,
	},
	RateLimiter: rateLimiter,
	WorkerCount: 100,
}

type DateConfig struct {
	Date string `json:"date"`
}

func (e SimpleEngine) Run() {

	contents, err := ioutil.ReadFile("./config.json")
	if err != nil {
		panic(err)
	}
	var config DateConfig
	err = json.Unmarshal(contents, &config)
	if err != nil {
		panic(err)
	}

	start, err := time.Parse("2006-01-02", config.Date)
	if err != nil {
		panic(err)
	}

start:
	// 初始化计数器
	persist.FlightSum = 0
	persist.AirportIndex = 0
	types.T1 = time.Now()

	// 获取新的日期
	date := start.Format("2006-01-02")
	// generate airport seed
	airports, err := store.AirportChanForForeign()
	if err != nil {
		panic(err)
	}

	// configure scheduler's in channel
	// this filter will generate tomorrow flight request
	reqChannel := umetrip.ListRequestPipe(airports, date)
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

	timer := time.NewTimer(5 * time.Minute)
	completed := make(chan bool)
	for {
		timer.Reset(5 * time.Minute)

		// when all result have been handled, this will blocked forever.
		// so, here use `select` to avoid this problem.
		select {
		case result := <-out:
			// this is only print to console/http client,
			// not save to database.
			//end := persist.Print(result, e.PrintNotifier, e.RateLimiter)
			//if end {
			//	fmt.Println("\nbegin next date...")
			//	time.Sleep(5 * time.Second)
			//	completed <- true
			//}

			// this is save to database
			go func() {
				data, end, err := persist.Save(result, true, e.PrintNotifier, e.RateLimiter)
				if err != nil {
					log.Printf("\nsave %v error: %v\n", data, err)
				}
				if end {
					fmt.Println("\nbegin next date...")
					time.Sleep(2 * time.Second)
					completed <- true
				}
			}()

		case <-timer.C:
			start = start.Add(24 * time.Hour)
			goto start
		case <-completed:
			start = start.Add(24 * time.Hour)
			goto start
		}
	}
}

func (e SimpleEngine) fetchWorker(r types.Request) (types.ParseResult, error) {
	return fetcher.FetchRequest(r, e.RateLimiter)
}

func (e SimpleEngine) CreateFetchWorker(in chan types.Request, out chan types.ParseResult) {
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
