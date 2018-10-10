package main

import (
	"fmt"

	"time"

	"github.com/champkeh/crawler/ratelimiter"
	"github.com/champkeh/crawler/types"
)

func main() {
	rl := ratelimiter.NewSimpleRateLimiter(10)

	for i := 0; i < 1000; i++ {
		go fetch(rl)
	}

	time.Sleep(2 * time.Minute)
}

var counter = 0

func fetch(rateLimiter types.RateLimiter) {
	// limit fetch rate
	if rateLimiter != nil {
		rateLimiter.Wait()
	}
	counter++
	fmt.Println(counter)
}
