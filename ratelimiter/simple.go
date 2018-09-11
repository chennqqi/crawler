package ratelimiter

import (
	"log"
	"time"

	"sync"

	"github.com/champkeh/crawler/types"
)

type simpleRateLimiter struct {
	Rate     uint
	Fastest  uint
	Slowest  uint
	RateTick <-chan time.Time
	sync.Mutex
}

func NewSimpleRateLimiter(rate uint) types.RateLimiter {
	if rate < 5 || rate > 100 {
		panic("rate value is invalid(5~100)")
	}
	return &simpleRateLimiter{
		Rate:     rate,
		Fastest:  30,
		Slowest:  5000,
		RateTick: time.Tick(time.Duration(rate) * time.Millisecond),
	}
}

func (r *simpleRateLimiter) Value() <-chan time.Time {
	return r.RateTick
}

func (r *simpleRateLimiter) RateValue() uint {
	return r.Rate
}

var rateLimiter = time.Tick(20 * time.Millisecond)

func (r *simpleRateLimiter) Faster() {
	<-rateLimiter

	r.Lock()
	defer r.Unlock()

	if r.Rate <= r.Fastest {
		return
	}
	if r.Rate >= 1500 {
		r.Rate -= 200
	} else if r.Rate >= 1000 {
		r.Rate -= 100
	} else if r.Rate >= 500 {
		r.Rate -= 50
	} else if r.Rate >= 100 {
		r.Rate -= 10
	} else {
		r.Rate -= 5
	}

	r.RateTick = time.Tick(time.Duration(r.Rate) * time.Millisecond)
}

func (r *simpleRateLimiter) Slower() {
	<-rateLimiter

	r.Lock()
	defer r.Unlock()

	if r.Rate >= r.Slowest {
		return
	}
	r.Rate += 5
	r.RateTick = time.Tick(time.Duration(r.Rate) * time.Millisecond)
}

func (r *simpleRateLimiter) Run() {
	var rate = time.Tick(10 * time.Second)
	for {
		select {
		case <-rate:

			r.Faster()
			log.Printf("\nCurrent Rate: %d\n", r.Rate)
		}
	}
}
