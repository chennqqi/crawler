package engine

import (
	"github.com/champkeh/crawler/ratelimiter"
)

// 全局速率限制器
var rateLimiter = ratelimiter.NewSimpleRateLimiter(30)
