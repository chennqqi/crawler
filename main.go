package main

import (
	_ "net/http/pprof"
)

func main() {

	// 国内未来航班列表
	// file: internal-list-engine
	//inter.DefaultSimpleEngine.Run()

	// 国际未来航班列表
	// file: foreign-list-engine
	//foreign.DefaultSimpleEngine.Run()

	//cron.DefaultRealTimeEngine.Run()
}
