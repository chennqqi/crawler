package common

import (
	"github.com/champkeh/crawler/ratelimiter"
)

// 全局速率限制器
var RateLimiter = ratelimiter.NewSimpleRateLimiter(30)
