package main

import (
	"github.com/champkeh/crawler/umetrip/engine/inter"
)

func main() {
	// 国内未来1天的航班详情
	// file: fetch-future-detail
	inter.DefaultFutureEngine.Run()

	// 国内航班实时抓取引擎
	// file: realtime-engine
	//engine.DefaultRealTimeEngine.Run()

	// 国内未来航班列表
	// file: internal-list-engine
	//engine.DefaultSimpleEngine.Run()

	// 国际未来1天的航班详情
	// file: fetch-future-detail
	//foreign.DefaultFutureEngine.Run()

	// 国际未来航班列表
	// file: foreign-list-engine
	//foreign.DefaultSimpleEngine.Run()
}
