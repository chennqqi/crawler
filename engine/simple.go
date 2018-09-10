package engine

import (
	"log"

	"fmt"

	"github.com/champkeh/crawler/fetcher"
	"github.com/champkeh/crawler/scheduler"
	"github.com/champkeh/crawler/seeds"
	"github.com/champkeh/crawler/types"
)

type SimpleEngine struct {
	Scheduler   Scheduler
	WorkerCount int
}

type Scheduler interface {
	Submit(types.Request)
	ConfigureMasterWorkerChan(chan types.Request)
}

var DefaultEngine = SimpleEngine{
	Scheduler:   &scheduler.SimpleScheduler{},
	WorkerCount: 100,
}

func (e SimpleEngine) Run() {
	airports, err := seeds.PullAirportList()
	if err != nil {
		panic(err)
	}

	in := seeds.AirportRequestFilter(airports)
	out := make(chan types.ParseResult)
	e.Scheduler.ConfigureMasterWorkerChan(in)

	for i := 0; i < e.WorkerCount; i++ {
		createWorker(in, out)
	}

	itemCount := 0
	for {
		result := <-out
		for _, item := range result.Items {
			fmt.Printf("Got item #%d: %v\n", itemCount, item)
			itemCount++
		}
	}
}

func worker(r types.Request) (types.ParseResult, error) {
	log.Printf("Fetching %s", r.Url)
	body, err := fetcher.Fetch(r.Url)
	if err != nil {
		log.Printf("Fetcher: error fetching url %s: %v)", r.Url, err)
		return types.ParseResult{}, err
	}

	return r.ParserFunc(body), nil
}

func createWorker(in chan types.Request, out chan types.ParseResult) {
	go func() {
		for {
			request := <-in
			parseResult, err := worker(request)
			if err != nil {
				continue
			}
			out <- parseResult
		}
	}()
}
