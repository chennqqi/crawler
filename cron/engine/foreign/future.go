package foreign

import (
	"fmt"
	"time"

	"github.com/champkeh/crawler/fetcher"
	"github.com/champkeh/crawler/notifier"
	"github.com/champkeh/crawler/persist"
	"github.com/champkeh/crawler/scheduler"
	"github.com/champkeh/crawler/source/umetrip"
	"github.com/champkeh/crawler/store"
	"github.com/champkeh/crawler/types"
	"github.com/labstack/gommon/log"
)

// 国际航班未来1天航班详情获取引擎
//
// 这个引擎用来抓取未来航班的详情数据(机型、前序航班信息、...)
// 只需要抓取未来1天的数据即可，因为只有未来1天的航班有前序航班信息
type FutureEngine struct {
	Scheduler     types.RequestScheduler
	PrintNotifier types.PrintNotifier
	RateLimiter   types.RateLimiter
	WorkerCount   int
}

// DefaultFutureEngine
//
// FutureEngine 的默认实现
var DefaultFutureEngine = FutureEngine{
	Scheduler: &scheduler.SimpleRequestScheduler{},
	PrintNotifier: &notifier.ConsolePrintNotifier{
		RateLimiter: rateLimiter,
	},

	// 采用全局的 rateLimiter
	RateLimiter: rateLimiter,
	WorkerCount: 100,
}

// Run 运行引擎
func (e FutureEngine) Run() {

	// 清除之前的数据
	persist.ClearDataBase(true)

	// 因为要作为计划任务每天执行，所以日期使用明天
	var date = time.Now().Add(24 * time.Hour).Format("2006-01-02")

	// 从未来国际航班列表中拉取要抓取的航班列表
	flightlist, err := store.FlightListChanAt(date, true)
	if err != nil {
		panic(err)
	}

	// configure scheduler's in channel
	// this filter will generate tomorrow flight request
	reqChannel := umetrip.DetailRequestPipe(flightlist)
	e.Scheduler.ConfigureRequestChan(reqChannel)

	// configure scheduler's out channel, has 100 space buffer channel
	out := make(chan types.ParseResult, 100)

	// create fetch worker
	for i := 0; i < e.WorkerCount; i++ {
		e.CreateFetchWorker(reqChannel, out)
	}

	// run the print-notifier
	go e.PrintNotifier.Run()

	// run the rate-limiter
	go e.RateLimiter.Run()

	timer := time.NewTimer(3 * time.Minute)

	for {
		timer.Reset(3 * time.Minute)

		// when all result have been handled, this will blocked forever.
		// so, here use `select` to avoid this problem.
		select {
		case result := <-out:
			//persist.PrintDetail(result, e.PrintNotifier, e.RateLimiter)

			// this is save to database
			go func() {
				data, err := persist.SaveDetail(result, true, e.PrintNotifier, e.RateLimiter)
				if err != nil {
					log.Warnf("save %v error: %v", data, err)
				}
			}()

		case <-timer.C:
			fmt.Println("Read timeout, exit the program.")
			return
		}
	}
}

func (e FutureEngine) fetchWorker(r types.Request) (types.ParseResult, error) {
	return fetcher.FetchRequest(r, e.RateLimiter)
}

func (e FutureEngine) CreateFetchWorker(in chan types.Request, out chan types.ParseResult) {
	go func() {
		for {
			request := <-in
			parseResult, err := e.fetchWorker(request)
			if err != nil {
				// fetch request failed, submit this request to scheduler to fetch
				// later again.
				e.Scheduler.Submit(request)

				// slow down the rate-limiter
				e.RateLimiter.Slower()
				continue
			}
			// out-channel has 100 buffer space
			out <- parseResult
		}
	}()
}
