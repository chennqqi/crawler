package main

import (
	"github.com/champkeh/crawler/realtime"
)

func main() {

	// 国内航班实时抓取引擎
	// file: realtime-engine
	realtime.DefaultRealTimeEngine.Run()

	// 国内未来航班列表
	// file: internal-list-engine
	//inter.DefaultSimpleEngine.Run()

	// 国际未来航班列表
	// file: foreign-list-engine
	//foreign.DefaultSimpleEngine.Run()

	//cron.DefaultRealTimeEngine.Run()
}
