package engine

import (
	"log"

	"time"

	"fmt"

	"github.com/champkeh/crawler/fetcher"
	"github.com/champkeh/crawler/notifier"
	"github.com/champkeh/crawler/persist"
	"github.com/champkeh/crawler/ratelimiter"
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
	Scheduler:     &scheduler.SimpleScheduler{},
	PrintNotifier: &notifier.ConsolePrintNotifier{},
	RateLimiter:   ratelimiter.NewSimpleRateLimiter(50),
	WorkerCount:   100,
}

// Run is the first step to startup the engine.
// this is used to fetch the first batch flight list data
// only once every day, and save result to database
func (e SimpleEngine) Run() {
	// generate airport seed
	airports, err := seeds.PullAirportList()
	if err != nil {
		panic(err)
	}

	// configure scheduler's in channel
	// this filter will generate tomorrow flight request
	reqChannel := seeds.AirportRequestFilter(airports)
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
			// this is only print to console/http client,
			// not save to database.
			//persist.Print(result, e.PrintNotifier, e.RateLimiter)

			// this is save to database
			go func() {
				data, err := persist.Save(result, e.PrintNotifier, e.RateLimiter)
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

func (e SimpleEngine) fetchWorker(r types.Request) (types.ParseResult, error) {
	//log.Printf("Fetching %s", r.Url)
	body, err := fetcher.Fetch(r.Url, e.RateLimiter)
	if err != nil {
		log.Printf("\nFetcher: error fetching url %s: %v\n", r.Url, err)
		log.Printf("Current Rate: %d\n", e.RateLimiter.Rate())
		return types.ParseResult{}, err
	}

	result := r.ParserFunc(body)
	result.RawParam = r.RawParam

	return result, nil
}

func (e SimpleEngine) CreateFetchWorker(in chan types.Request, out chan types.ParseResult) {
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
