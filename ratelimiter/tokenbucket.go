package ratelimiter

import (
	"time"

	"sync"

	"github.com/champkeh/crawler/types"
	"golang.org/x/net/context"
	"golang.org/x/time/rate"
)

type tokenBucketRateLimiter struct {
	Limiter *rate.Limiter
	Fastest rate.Limit
	Slowest rate.Limit
	sync.Mutex
}

func NewTokenBucketRateLimiter(r int, b int) types.RateLimiter {
	return &tokenBucketRateLimiter{
		Limiter: rate.NewLimiter(rate.Limit(r), b),
		Fastest: rate.Limit(40),
		Slowest: rate.Limit(0.2),
	}
}

func (lim *tokenBucketRateLimiter) Run() {
	ticker := time.Tick(10 * time.Second)
	for {
		select {
		case <-ticker:

			lim.Faster()
		default:
		}
	}
}

func (lim *tokenBucketRateLimiter) Faster() {
	lim.Lock()
	defer lim.Unlock()

	if lim.Limiter.Limit() >= lim.Fastest {
		return
	}

	lim.Limiter.SetLimit(lim.Limiter.Limit() + 1)
}

func (lim *tokenBucketRateLimiter) Slower() {
	lim.Lock()
	defer lim.Unlock()

	if lim.Limiter.Limit() <= lim.Slowest {
		return
	}

	lim.Limiter.SetLimit(lim.Limiter.Limit() - 0.1)
}

func (lim *tokenBucketRateLimiter) Wait() {
	err := lim.Limiter.Wait(context.Background())
	if err != nil {
		panic(err)
	}
}

func (lim *tokenBucketRateLimiter) QPS() float64 {
	return float64(lim.Limiter.Limit())
}

func (lim *tokenBucketRateLimiter) Rate() uint {
	return uint(1000 / lim.Limiter.Limit())
}
