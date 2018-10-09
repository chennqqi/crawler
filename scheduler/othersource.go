package scheduler

import (
	"errors"

	"fmt"

	"time"

	"github.com/champkeh/crawler/fetcher"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/veryzhun/parser"

	"github.com/champkeh/crawler/utils"
	"github.com/labstack/gommon/log"
)

var (
	LogFile = "othersource.log"
)

type OtherSourceScheduler struct {
	requestChan chan types.Request
}

func (s *OtherSourceScheduler) ConfigureRequestChan(channel chan types.Request) {
	s.requestChan = channel
}

func (s *OtherSourceScheduler) Submit(req types.Request) {
	if s.requestChan == nil {
		panic(errors.New("before submit request to scheduler, you must" +
			" configure the request-channel for this scheduler"))
	}
	go func() {
		s.requestChan <- req
	}()
}

func (s *OtherSourceScheduler) Run() {
	utils.MustExist(LogFile)
	//
	ticker := time.NewTicker(1 * time.Minute)
	for {
		select {
		case <-ticker.C:
			request := <-s.requestChan

			// 切换成飞常准数据源
			request.ParserFunc = parser.ParseDetail
			request.Url = fmt.Sprintf("http://webapp.veryzhun.com/h5/flightsearch?"+
				"fnum=%s&date=%s&token=5cf2036c3db9fe08a7ee0c9b2077d37d", request.RawParam.Fno, request.RawParam.Date)
			fmt.Printf("fetch %s:%s\n", request.RawParam.Date, request.RawParam.Fno)

			result, err := fetcher.FetchWorker(request, nil)
			if err != nil {
				log.Print(err)
			}
			for _, item := range result.Items {
				utils.AppendToFile(LogFile,
					fmt.Sprintf("[%s] [%s:%s %v]\n",
						time.Now().Format("2006-01-02 15:04:05"),
						result.Request.RawParam.Date, result.Request.RawParam.Fno,
						item))
			}

		}
	}
}
