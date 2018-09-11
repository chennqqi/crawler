package engine

import (
	"log"

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
	Saver         types.Saver
	PrintNotifier types.PrintNotifier
	RateLimiter   types.RateLimiter
	WorkerCount   int
}

var DefaultEngine = SimpleEngine{
	Scheduler:     &scheduler.SimpleScheduler{},
	Saver:         &persist.Saver{},
	PrintNotifier: &notifier.ConsolePrintNotifier{},
	RateLimiter:   ratelimiter.NewSimpleRateLimiter(50),
	WorkerCount:   100,
}

// Setup is the first step to startup the engine.
// this is used to fetch the first batch flight list data
// only once every day, and save to database
func (e SimpleEngine) Setup() {
	// generate airport seed
	airports, err := seeds.PullAirportList()
	if err != nil {
		panic(err)
	}

	// configure scheduler's in channel
	reqChannel := seeds.AirportRequestFilter(airports)
	e.Scheduler.ConfigureRequestChan(reqChannel)

	// configure scheduler's out channel, non-buffer channel
	out := make(chan types.ParseResult)

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

	for {
		// when all result have been handled, this will blocked forever.
		// so, here should use `select` to avoid this problem.
		// and, when all result have been saved/printed, this goroutine
		// will exit and run again after 24 hour(e.g. tomorrow)
		// need close scheduler's in-channel to clean the worker goroutines.
		result := <-out

		// this is only print to console/http client,
		// not save to database.
		persist.Print(result, e.PrintNotifier)

		// this is save to database
		//go func() {
		//	data, err := persist.Save(result)
		//	if err != nil {
		//		log.Printf("\nsave %v error: %v\n", data, err)
		//	}
		//}()
	}
}

// Run is used to fetch the flight data which duration the next 2 hours,
// and execute every 10 minutes.
func (e SimpleEngine) Run() {

}

func (e SimpleEngine) fetchWorker(r types.Request) (types.ParseResult, error) {
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

func (e SimpleEngine) CreateFetchWorker(in chan types.Request, out chan types.ParseResult) {
	go func() {
		for {
			// when all request have been handled, this will blocked forever.
			// so, here should use `select` to avoid this problem.
			request, ok := <-in
			if !ok {
				// request chan is closed, so exit the worker goroutine
				return
			}
			parseResult, err := e.fetchWorker(request)
			if err != nil {
				// 请求处理出错，继续加入到 in channel 中处理
				e.Scheduler.Submit(request)

				// todo: 实现某种机制来动态调整rateLimiter
				e.RateLimiter.Slower()
				continue
			}

			// because out channel is non-buffer, so this probably blocked if
			// out channel has value not handled.
			out <- parseResult
		}
	}()
}
