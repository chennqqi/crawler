package main

import (
	"github.com/champkeh/crawler/engine"
)

func main() {
	// 未来1天的航班详情
	//engine.DefaultFutureEngine.Run()

	// 实时航班
	engine.DefaultRealTimeEngine.Run()

	// 未来航班列表
	//engine.DefaultSimpleEngine.Run()
}
