package scheduler

import (
	"errors"

	"github.com/champkeh/crawler/types"
)

// SimpleAirportScheduler 机场调度器
//
// 机场调度器用于调度airport在3个数据源之间进行切换
// 由机场可获取对应数据源的列表request
// request := source.ListRequest(airport)
// result, err := fetcher.FetchRequest(request, rateLimiter)
type SimpleAirportScheduler struct {
	airportChan chan types.Airport
}

// ConfigureAirportChan 配置机场调度器的 airport 通道
func (s *SimpleAirportScheduler) ConfigureAirportChan(channel chan types.Airport) {
	s.airportChan = channel
}

// Submit 向机场调度器中添加新的机场
func (s *SimpleAirportScheduler) Submit(airport types.Airport) {
	if s.airportChan == nil {
		panic(errors.New("before submit airport to scheduler, you must" +
			" configure the airport-channel for this scheduler"))
	}
	go func() {
		s.airportChan <- airport
	}()
}
