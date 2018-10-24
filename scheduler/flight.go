package scheduler

import (
	"errors"

	"github.com/champkeh/crawler/types"
)

// SimpleFlightScheduler 航班调度器
//
// 航班调度器用于调度航班在3个数据源之间进行切换
// 由航班可获取对应数据源的详情request
// request := source.DetailRequest(flight)
// result, err := fetcher.FetchRequest(request, rateLimiter)
type SimpleFlightScheduler struct {
	flightChan chan types.FlightInfo
}

// ConfigureFlightChan 配置航班调度器的 flight 通道
func (s *SimpleFlightScheduler) ConfigureFlightChan(channel chan types.FlightInfo) {
	s.flightChan = channel
}

// Submit 向航班调度器中添加新的航班
func (s *SimpleFlightScheduler) Submit(flight types.FlightInfo) {
	if s.flightChan == nil {
		panic(errors.New("before submit flight to scheduler, you must" +
			" configure the flight-channel for this scheduler"))
	}
	go func() {
		s.flightChan <- flight
	}()
}
