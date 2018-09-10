package main

import (
	"github.com/champkeh/crawler/engine"
	"github.com/champkeh/crawler/scheduler"
	"github.com/champkeh/crawler/umetrip/parser"
)

func main() {
	e := engine.ConcurrentEngine{
		Scheduler:    &scheduler.SimpleScheduler{},
		WorkderCount: 10,
	}

	e.Run(engine.Request{
		Url:        "http://www.umetrip.com/mskyweb/fs/fa.do?dep=SHA&arr=PEK&date=2018-09-09",
		ParserFunc: parser.ParseList,
	}, engine.Request{
		Url:        "http://www.umetrip.com/mskyweb/fs/fc.do?flightNo=CA1858&date=2018-09-09",
		ParserFunc: parser.ParseDetail,
	})
}
