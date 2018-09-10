package engine

import (
	"log"

	"github.com/champkeh/crawler/fetcher"
	"github.com/champkeh/crawler/persist"
	"github.com/champkeh/crawler/scheduler"
	"github.com/champkeh/crawler/seeds"
	"github.com/champkeh/crawler/types"
)

type SimpleEngine struct {
	Scheduler   Scheduler
	Saver       Saver
	WorkerCount int
}

type Scheduler interface {
	Submit(types.Request)
	ConfigureRequestChan(chan types.Request)
}

type Saver interface {
	Submit(types.ParseResult)
	ConfigureParseResultChan(chan types.ParseResult)
}

var DefaultEngine = SimpleEngine{
	Scheduler:   &scheduler.SimpleScheduler{},
	Saver:       &persist.Saver{},
	WorkerCount: 100,
}

// Run startup the engine
func (e SimpleEngine) Run() {
	// generate seed
	airports, err := seeds.PullAirportList()
	if err != nil {
		panic(err)
	}

	// configure in channel
	reqChannel := seeds.AirportRequestFilter(airports)
	e.Scheduler.ConfigureRequestChan(reqChannel)

	// out channel
	out := make(chan types.ParseResult)

	// create fetch worker
	for i := 0; i < e.WorkerCount; i++ {
		e.CreateFetchWorker(reqChannel, out)
	}

	for {
		result := <-out
		err := persist.Save(result)
		if err != nil {
			log.Printf("\nsave error: %v\n", err)
			continue
		}
	}
}

func fetchWorker(r types.Request) (types.ParseResult, error) {
	//log.Printf("Fetching %s", r.Url)
	body, err := fetcher.Fetch(r.Url)
	if err != nil {
		log.Printf("\nFetcher: error fetching url %s: %v\n", r.Url, err)
		return types.ParseResult{}, err
	}

	result := r.ParserFunc(body)
	result.Dep = r.Dep
	result.Arr = r.Arr
	result.Date = r.Date

	return result, nil
}

func (e SimpleEngine) CreateFetchWorker(in chan types.Request, out chan types.ParseResult) {
	go func() {
		for {
			request, ok := <-in
			if !ok {
				return
			}
			parseResult, err := fetchWorker(request)
			if err != nil {
				// 请求处理出错，继续加入到 workchannel 中处理
				e.Scheduler.Submit(request)
				continue
			}
			out <- parseResult
		}
	}()
}
