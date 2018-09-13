package ratelimiter

import (
	"time"

	"sync"

	"github.com/champkeh/crawler/types"
)

type simpleRateLimiter struct {
	Fastest uint
	Slowest uint

	rate     uint
	rateTick <-chan time.Time

	sync.Mutex
}

func (r *simpleRateLimiter) QPS() float64 {
	r.Lock()
	defer r.Unlock()

	return 1000 / float64(r.rate)
}

func (r *simpleRateLimiter) Rate() uint {
	r.Lock()
	defer r.Unlock()

	return r.rate
}

func (r *simpleRateLimiter) Wait() {
	<-r.rateTick
}

func NewSimpleRateLimiter(rate uint) types.RateLimiter {
	if rate < 30 || rate > 5000 {
		panic("rate value is invalid(30~5000)")
	}
	return &simpleRateLimiter{
		rate:     rate,
		Fastest:  30,
		Slowest:  5000,
		rateTick: time.Tick(time.Duration(rate) * time.Millisecond),
	}
}

var rateLimiter = time.Tick(20 * time.Millisecond)

func (r *simpleRateLimiter) Faster() {
	<-rateLimiter

	r.Lock()
	defer r.Unlock()

	if r.rate <= r.Fastest {
		return
	}
	if r.rate >= 1500 {
		r.rate -= 200
	} else if r.rate >= 1000 {
		r.rate -= 100
	} else if r.rate >= 500 {
		r.rate -= 50
	} else if r.rate >= 100 {
		r.rate -= 10
	} else {
		r.rate -= 5
	}

	r.rateTick = time.Tick(time.Duration(r.rate) * time.Millisecond)
}

func (r *simpleRateLimiter) Slower() {
	<-rateLimiter

	r.Lock()
	defer r.Unlock()

	if r.rate >= r.Slowest {
		return
	}
	r.rate += 5
	r.rateTick = time.Tick(time.Duration(r.rate) * time.Millisecond)
}

// Run is used for increase the rate limiter's rate value.
// auto call Faster() method per 10 seconds
func (r *simpleRateLimiter) Run() {
	var rate = time.Tick(10 * time.Second)
	for {
		select {
		case <-rate:
			r.Faster()
		default:

		}
	}
}
