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
	QPS() float64
	Rate() uint
}

type Runner interface {
	Run()
}

type Param struct {
	Dep  string
	Arr  string
	Fno  string
	Date string
}

// Request
type Request struct {
	RawParam   Param
	FetchCount int64
	Url        string
	ParserFunc func([]byte) (ParseResult, error)
}

// ParseResult
type ParseResult struct {
	Request Request
	Items   []interface{}
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
	Type         string
	Elapsed      time.Duration
	Date         string
	Airport      Airport
	AirportIndex int
	AirportTotal int
	FlightCount  int
	FlightSum    int
	FlightTotal  int
	Progress     float32
	QPS          float64
}

func (o NotifyData) String() string {
	if o.Type == "detail" {
		return fmt.Sprintf("%v [Flight %s #%d/%d/%d %.2f%%] [Rate:%.2fqps]",
			o.Elapsed, o.Date, o.FlightSum, o.FlightTotal, o.FlightCount, o.Progress, o.QPS)
	} else if o.Type == "list" {
		return fmt.Sprintf("%v [Airport %s #%d/%d %.2f%%](%s->%s): [items %d/%d Rate:%.2fqps]",
			o.Elapsed, o.Date, o.AirportIndex, o.AirportTotal, o.Progress, o.Airport.DepCode, o.Airport.ArrCode,
			o.FlightSum, o.FlightCount, o.QPS)
	} else {
		return fmt.Sprintf("type error:%s", o.Type)
	}
}
