package engine

import (
	"log"

	"time"

	"fmt"

	"encoding/json"
	"io/ioutil"

	"github.com/champkeh/crawler/fetcher"
	"github.com/champkeh/crawler/notifier"
	"github.com/champkeh/crawler/persist"
	"github.com/champkeh/crawler/scheduler"
	"github.com/champkeh/crawler/seeds"
	"github.com/champkeh/crawler/types"
)

type SimpleEngine struct {
	Scheduler     types.Scheduler
	PrintNotifier types.PrintNotifier
	RateLimiter   types.RateLimiter
	WorkerCount   int
}

var DefaultSimpleEngine = SimpleEngine{
	Scheduler: &scheduler.SimpleScheduler{},
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
	date := start.Format("2006-01-02")
	// generate airport seed
	airports, err := seeds.PullAirportList()
	if err != nil {
		panic(err)
	}

	// configure scheduler's in channel
	// this filter will generate tomorrow flight request
	reqChannel := seeds.AirportRequestFilter(airports, date)
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
	completed := make(chan bool)
	for {
		timer.Reset(3 * time.Minute)

		// when all result have been handled, this will blocked forever.
		// so, here use `select` to avoid this problem.
		select {
		case result := <-out:
			// this is only print to console/http client,
			// not save to database.
			//end := persist.Print(result, e.PrintNotifier, e.RateLimiter)
			//if end {
			//	close(reqChannel)
			//	fmt.Println("begin next date")
			//	time.Sleep(5 * time.Second)
			//	completed <- true
			//}

			// this is save to database
			go func() {
				data, end, err := persist.Save(result, e.PrintNotifier, e.RateLimiter)
				if err != nil {
					log.Printf("\nsave %v error: %v\n", data, err)
				}
				if end {
					close(reqChannel)
					fmt.Println("begin next date")
					time.Sleep(5 * time.Second)
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
	body, err := fetcher.Fetch(r.Url, e.RateLimiter)
	if err != nil {
		log.Printf("\nFetcher: error fetching url %s: %v\n", r.Url, err)
		log.Printf("Current Rate: %d\n", e.RateLimiter.Rate())
		return types.ParseResult{}, err
	}

	result, err := r.ParserFunc(body)
	if err != nil {
		log.Printf("parse (%s:%s->%s) error: %v\n",
			r.RawParam.Date, r.RawParam.Dep, r.RawParam.Arr, err)
	}
	result.Request = r

	return result, nil
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
