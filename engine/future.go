package engine

import (
	"fmt"
	"log"
	"time"

	"github.com/champkeh/crawler/fetcher"
	"github.com/champkeh/crawler/persist"
	"github.com/champkeh/crawler/seeds"
	"github.com/champkeh/crawler/types"
)

// this engine is used to fetch future flight data
type FutureEngine struct {
	Scheduler     types.Scheduler
	PrintNotifier types.PrintNotifier
	RateLimiter   types.RateLimiter
	WorkerCount   int
}

// Setup is the first step to startup the engine.
// this is used to fetch the first batch flight list data
// only once every day, and save result to database
// when completed this will exit and should run after 24 hour(e.g. tomorrow)
func (e FutureEngine) Run() {
	// generate airport seed
	flightlist, err := seeds.PullFlightListAt("2018-09-13")
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

	// configure print notify channel
	// this channel is used for cache the notify data, and have
	// 100 buffer space.
	printChan := make(chan types.NotifyData, 100)
	e.PrintNotifier.ConfigureChan(printChan)
	go e.PrintNotifier.Run()

	// run the rate limiter
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
			//persist.Print(result, e.PrintNotifier)

			// this is save to database
			go func() {
				data, err := persist.Save(result, e.PrintNotifier)
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
	//log.Printf("Fetching %s", r.Url)
	body, err := fetcher.Fetch(r.Url, e.RateLimiter.Value())
	if err != nil {
		log.Printf("\nFetcher: error fetching url %s: %v\n", r.Url, err)
		log.Printf("Current Rate: %d\n", e.RateLimiter.RateValue())
		return types.ParseResult{}, err
	}

	result := r.ParserFunc(body)
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
