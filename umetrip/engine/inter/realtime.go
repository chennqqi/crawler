package inter

import (
	"time"

	"fmt"

	"github.com/champkeh/crawler/common"
	"github.com/champkeh/crawler/fetcher"
	"github.com/champkeh/crawler/notifier"
	"github.com/champkeh/crawler/persist"
	"github.com/champkeh/crawler/scheduler"
	"github.com/champkeh/crawler/seeds"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/utils"
	"github.com/labstack/gommon/log"
)

var (
	LogFile = "realtime.log"
)

// RealTimeEngine
//
// 这个引擎用于爬取实时航班数据
type RealTimeEngine struct {
	Scheduler            types.Scheduler
	PrintNotifier        types.PrintNotifier
	RateLimiter          types.RateLimiter
	WorkerCount          int
	OtherSourceScheduler *scheduler.OtherSourceScheduler
}

// DefaultRealTimeEngine
//
// 实时引擎的默认配置
var DefaultRealTimeEngine = RealTimeEngine{
	Scheduler: &scheduler.SimpleScheduler{},
	PrintNotifier: &notifier.ConsolePrintNotifier{
		RateLimiter: common.RateLimiter,
	},
	RateLimiter:          common.RateLimiter,
	WorkerCount:          100,
	OtherSourceScheduler: &scheduler.OtherSourceScheduler{},
}

// Run
//
// 启动实时抓取引擎
func (e RealTimeEngine) Run() {

	// 确保日志文件存在
	utils.MustExist(LogFile)

	// 实时抓取请求的容器
	reqChannel := make(chan types.Request, 5000)

	// 从实时航班列表中拉取未来2小时起飞的航班，放在 reqChannel 容器中
	// note: 由于数据源的问题，可能会拉取到不在2小时之内的航班
	err := seeds.PullLatestFlight(reqChannel, true)
	if err != nil {
		panic(err)
	}

	go func() {
		// 然后，每隔2小时拉取一次
		ticker := time.NewTicker(100 * time.Minute)
		for {
			select {
			case <-ticker.C:
				// 拉取最近2小时起飞的航班，放在 reqChannel 容器中
				err := seeds.PullLatestFlight(reqChannel, false)
				if err != nil {
					panic(err)
				}
			}
		}
	}()

	e.Scheduler.ConfigureRequestChan(reqChannel)
	e.OtherSourceScheduler.ConfigureRequestChan(make(chan types.Request, 1000))

	// configure scheduler's out channel, has 100 space buffer channel
	out := make(chan types.ParseResult, 1000)

	// create fetch worker
	for i := 0; i < e.WorkerCount; i++ {
		e.CreateFetchWorker(reqChannel, out)
	}

	// run the rate-limiter
	go e.RateLimiter.Run()
	go e.OtherSourceScheduler.Run()

	for {
		select {
		case result := <-out:
			finished, status := persist.PrintRealTime(result, e.RateLimiter, reqChannel)

			// 根据结果决定是否要重新添加到任务队列
			go func(finished bool, status string, result types.ParseResult) {
				if finished == false {
					// 任务未完成
					// todo: 正常的航班也有可能状态突然变成暂无，因此需要考虑这种情况
					if status == "暂无" && result.Request.FetchCount <= 10 {
						// 暂无状态的航班，10分钟之后再次请求
						time.Sleep(10 * time.Minute)
						result.Request.FetchCount++
						e.Scheduler.Submit(result.Request)

						utils.AppendToFile(LogFile,
							fmt.Sprintf("==2:[%s]no status entry [%s:%s %d]\n",
								time.Now().Format("2006-01-02 15:04:05"),
								result.Request.RawParam.Date, result.Request.RawParam.Fno,
								result.Request.FetchCount))
					} else if status == "暂无" {
						// 如果连续出现暂无次数大于50，则不再检测
						utils.AppendToFile(LogFile,
							fmt.Sprintf("==3:[%s]ignore no status entry [%s:%s %d]\n",
								time.Now().Format("2006-01-02 15:04:05"),
								result.Request.RawParam.Date, result.Request.RawParam.Fno,
								result.Request.FetchCount))

						// todo: 需要使用备用源进行查询
						e.OtherSourceScheduler.Submit(result.Request)
					} else {
						// 中间状态的航班，5分钟之后继续监测
						time.Sleep(5 * time.Minute)
						// 抓取正常，则重置该参数
						result.Request.FetchCount = 0
						e.Scheduler.Submit(result.Request)

						utils.AppendToFile(LogFile,
							fmt.Sprintf("==1:[%s]intermediate status entry [%s:%s %s %d]\n",
								time.Now().Format("2006-01-02 15:04:05"),
								result.Request.RawParam.Date, result.Request.RawParam.Fno,
								status, result.Request.FetchCount))
					}
				} else {
					// 任务完成
					// note: 此处需要判断 status 是否为完成状态。如果 status 为空，则有可能是此处请求失败，
					// 需要重新请求
					if status == "" && result.Request.FetchCount <= 10 {
						// 不确定是否结束，继续添加到队列
						time.Sleep(10 * time.Minute)
						result.Request.FetchCount++
						e.Scheduler.Submit(result.Request)

						utils.AppendToFile(LogFile,
							fmt.Sprintf("==4:[%s]empty status entry [%s:%s %d]\n",
								time.Now().Format("2006-01-02 15:04:05"),
								result.Request.RawParam.Date, result.Request.RawParam.Fno,
								result.Request.FetchCount))
					} else if status == "" {
						// 如果连续出现暂无次数大于50，则不再检测
						utils.AppendToFile(LogFile,
							fmt.Sprintf("==3:[%s]ignore no status entry [%s:%s %d]\n",
								time.Now().Format("2006-01-02 15:04:05"),
								result.Request.RawParam.Date, result.Request.RawParam.Fno,
								result.Request.FetchCount))

						// todo: 需要使用备用源进行查询
						e.OtherSourceScheduler.Submit(result.Request)
					} else {
						// 确认结束，无操作
						utils.AppendToFile(LogFile,
							fmt.Sprintf("==5:[%s]complete status entry [%s:%s %s %d]\n",
								time.Now().Format("2006-01-02 15:04:05"),
								result.Request.RawParam.Date, result.Request.RawParam.Fno,
								status, result.Request.FetchCount))
					}
				}
			}(finished, status, result)
		}
	}
}

func (e RealTimeEngine) CreateFetchWorker(in chan types.Request, out chan types.ParseResult) {
	go func() {
		for {
			request := <-in

			edge := time.Now().Add(-2 * 24 * time.Hour).Format("2006-01-02")

			if request.RawParam.Date <= edge {
				// 前天的航班不再跟踪
				utils.AppendToFile(LogFile,
					fmt.Sprintf(">=2:[%s]ignore past entry [%s:%s %d]\n",
						time.Now().Format("2006-01-02 15:04:05"),
						request.RawParam.Date, request.RawParam.Fno, request.FetchCount))
				continue
			} else if request.RawParam.Date < time.Now().Format("2006-01-02") {
				// 昨天的航班，写入一条警告日志，但需要继续跟踪
				// note: 在跨天的时候会输出大量信息
				utils.AppendToFile(LogFile,
					fmt.Sprintf(">=1:[%s]past entry [%s:%s %d]\n",
						time.Now().Format("2006-01-02 15:04:05"),
						request.RawParam.Date, request.RawParam.Fno, request.FetchCount))
			}

			parseResult, err := fetcher.FetchWorker(request, e.RateLimiter)
			if err != nil {
				// fetch request failed, submit this request to scheduler to fetch
				// later again.
				log.Warnf("fetch worker err: %s", err)
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
