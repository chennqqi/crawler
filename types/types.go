package types

import (
	"fmt"
	"time"
)

type Scheduler interface {
	Submit(Request)
	ConfigureRequestChan(chan Request)
}

type PrintNotifier interface {
	Runner

	Print(data NotifyData)
}

type RateLimiter interface {
	Runner

	Faster()
	Slower()
	Wait()
	RateValue() uint
}

type Runner interface {
	Run()
}

type Param struct {
	Dep  string
	Arr  string
	Date string
}

// Request
type Request struct {
	RawParam   Param
	Url        string
	ParserFunc func([]byte) ParseResult
}

// ParseResult
type ParseResult struct {
	RawParam Param
	Items    []interface{}
}

func NilParser(contents []byte) ParseResult {
	return ParseResult{}
}

// Airport come from database seed
type Airport struct {
	DepCode string
	ArrCode string
}

func init() {
	T1 = time.Now()
}

var (
	// T1 is for runtime statistics
	T1 time.Time
)

// Output represent output statistics
type NotifyData struct {
	Elapsed      time.Duration
	Airport      Airport
	AirportIndex int
	AirportTotal int
	FlightCount  int
	FlightSum    int
	Progress     float32
	CurrentRate  uint
}

func (o NotifyData) String() string {
	return fmt.Sprintf("%v Airport #%d/%d (%s->%s): items %d; flight: %d/%.2f%% Rate:%d",
		o.Elapsed, o.AirportIndex, o.AirportTotal, o.Airport.DepCode, o.Airport.ArrCode,
		o.FlightCount, o.FlightSum, o.Progress, o.CurrentRate)
}
