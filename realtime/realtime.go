package realtime

import (
	"time"

	"fmt"

	"sync"

	"github.com/champkeh/crawler/fetcher"
	"github.com/champkeh/crawler/logs"
	"github.com/champkeh/crawler/persist"
	"github.com/champkeh/crawler/proxy/pool"
	"github.com/champkeh/crawler/ratelimiter"
	"github.com/champkeh/crawler/scheduler"
	"github.com/champkeh/crawler/source/ctrip"
	ctripParser "github.com/champkeh/crawler/source/ctrip/parser"
	"github.com/champkeh/crawler/source/umetrip"
	umetripParser "github.com/champkeh/crawler/source/umetrip/parser"
	"github.com/champkeh/crawler/source/veryzhun"
	veryParser "github.com/champkeh/crawler/source/veryzhun/parser"
	"github.com/champkeh/crawler/store"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/utils"
)

// RealTimeEngine
//
// 这个引擎用于从 航旅纵横/携程 爬取实时航班数据
type RealTimeEngine struct {
	// 航联纵横 umetrip 的航班容器的调度器
	UFlightScheduler types.FlightScheduler
	// 频率限制器
	URateLimiter types.RateLimiter

	// 携程 ctrip 的航班容器的调度器
	CFlightScheduler types.FlightScheduler
	// 频率限制器
	CRateLimiter types.RateLimiter

	// 飞常准 veryzhun 的航班容器的调度器
	VFlightScheduler types.FlightScheduler
	// 频率限制器
	VRateLimiter types.RateLimiter
	// 代理ip池
	ProxyPool *pool.LocalProxyPool

	WorkerCount int
	mutex       sync.Mutex
}

// DefaultRealTimeEngine
//
// 实时引擎的默认配置
var DefaultRealTimeEngine = RealTimeEngine{
	UFlightScheduler: &scheduler.SimpleFlightScheduler{},
	CFlightScheduler: &scheduler.SimpleFlightScheduler{},
	VFlightScheduler: &scheduler.SimpleFlightScheduler{},
	URateLimiter:     ratelimiter.NewSimpleRateLimiterFull(20, 5000, 30),
	CRateLimiter:     ratelimiter.NewSimpleRateLimiterFull(20, 5000, 30),
	VRateLimiter:     ratelimiter.NewSimpleRateLimiterFull(20, 5000, 50),

	ProxyPool:   &pool.LocalProxyPool{},
	WorkerCount: 100,
}

// Run
//
// 启动实时抓取引擎
func (e RealTimeEngine) Run() {

	// 确保日志文件存在
	utils.MustExist(logs.ULogFile)
	utils.MustExist(logs.CLogFile)
	utils.MustExist(logs.VLogFile)
	utils.MustExist(logs.InfoLogFile)
	utils.MustExist(logs.SaveInfoLogFile)

	// 航班容器
	uFlightChannel := make(chan types.FlightInfo, 5000)
	cFlightChannel := make(chan types.FlightInfo, 5000)
	vFlightChannel := make(chan types.FlightInfo, 3000)

	// 从实时航班列表中拉取未来2小时起飞的航班，放在 reqChannel 容器中
	// note: 由于数据源的问题，可能会拉取到不在2小时之内的航班
	err := store.PullLatestFlight(uFlightChannel, true)
	if err != nil {
		panic(err)
	}
	err = store.PullLatestFlight(cFlightChannel, true)
	if err != nil {
		panic(err)
	}

	go func() {
		// 然后，每隔2小时拉取一次
		ticker1 := time.NewTicker(100 * time.Minute)
		for {
			select {
			case <-ticker1.C:
				// 拉取最近2小时起飞的航班，放在 reqChannel 容器中
				err = store.PullLatestFlight(uFlightChannel, false)
				if err != nil {
					panic(err)
				}
				err = store.PullLatestFlight(cFlightChannel, false)
				if err != nil {
					panic(err)
				}
			}
		}
	}()

	e.UFlightScheduler.ConfigureFlightChan(uFlightChannel)
	e.CFlightScheduler.ConfigureFlightChan(cFlightChannel)
	e.VFlightScheduler.ConfigureFlightChan(vFlightChannel)

	// configure scheduler's out channel, has 100 space buffer channel
	out := make(chan types.ParseResult, 3000)

	// pipe channel
	for i := 0; i < e.WorkerCount; i++ {
		e.CreateUWorker(uFlightChannel, out)
	}
	for i := 0; i < e.WorkerCount; i++ {
		e.CreateCWorker(cFlightChannel, out)
	}
	for i := 0; i < e.WorkerCount; i++ {
		e.CreateVWorker(vFlightChannel, out)
	}

	// run the rate-limiter
	go e.URateLimiter.Run()
	go e.CRateLimiter.Run()
	go e.VRateLimiter.Run()
	go e.ProxyPool.Start()

	counter := 0
	ticker2 := time.NewTicker(1 * time.Second)
	for {
		select {
		case result := <-out:
			go persist.SaveToRealTime(result)
			counter++
			counter %= 10000

		case <-ticker2.C:
			fmt.Printf("\r[RealTime %d  UCh:%d  CCh:%d  VCh:%d Proxy:%d] [UR:%.2f  CR:%.2f  VR:%.2f]",
				counter, len(uFlightChannel), len(cFlightChannel), len(vFlightChannel), e.ProxyPool.Count(),
				e.URateLimiter.QPS(), e.CRateLimiter.QPS(), e.VRateLimiter.QPS())
		}
	}
}

// 航旅纵横的爬取逻辑总控
func (e RealTimeEngine) CreateUWorker(in chan types.FlightInfo, out chan types.ParseResult) {
	go func() {
		for {
			flight := <-in

			// 如果总的抓取次数大于阈值，则换用飞常准查询
			if flight.FetchCount >= 40 {
				flight.FetchCount = 0
				e.VFlightScheduler.Submit(flight)

				continue
			}
			// 连续出错2次， 该数据源无效，需要更换数据源
			if flight.FailCount >= 2 {
				utils.AppendToFile(logs.ULogFile,
					fmt.Sprintf(">=1:[%s]change source for entry [%s:%s %d/%d]\n",
						time.Now().Format("2006-01-02 15:04:05"),
						flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))

				// note: change data source
				flight.FailCount = 0
				e.VFlightScheduler.Submit(flight)

				continue
			}

			flight.FetchCount++

			// 航联纵横 数据源
			request := umetrip.DetailRequest(flight)
			result, err := fetcher.FetchRequest(request, e.URateLimiter)
			if err != nil {
				// 获取程序报错
				e.URateLimiter.Slower()
				utils.AppendToFile(logs.ULogFile,
					fmt.Sprintf("==1:[%s]fetch worker error:%q [%s:%s %d/%d]\n",
						time.Now().Format("2006-01-02 15:04:05"), err,
						flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))

				go func(flight types.FlightInfo) {
					time.Sleep(5 * time.Minute)

					// 失败次数累加
					flight.FailCount++
					e.UFlightScheduler.Submit(flight)
				}(flight)

				continue
			}

			if valid, state := ResultIsValid(result); valid {
				// 航班状态正常
				out <- result

				if ResultIsFinish(result) == false {
					// 航班没有结束
					utils.AppendToFile(logs.ULogFile,
						fmt.Sprintf("==2:[%s]fetch success entry [%s:%s %d/%d]\n",
							time.Now().Format("2006-01-02 15:04:05"),
							flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))

					go func(flight types.FlightInfo) {
						time.Sleep(5 * time.Minute)

						flight.FailCount = 0
						e.UFlightScheduler.Submit(flight)
					}(flight)
				} else {
					// 航班结束
					utils.AppendToFile(logs.ULogFile,
						fmt.Sprintf("==3:[%s]fetch finish entry [%s:%s %d/%d]\n",
							time.Now().Format("2006-01-02 15:04:05"),
							flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))
				}
			} else {
				// 航班状态异常
				utils.AppendToFile(logs.ULogFile,
					fmt.Sprintf("==4:[%s]fetch invalid status entry:%q [%s:%s %d/%d]\n",
						time.Now().Format("2006-01-02 15:04:05"), state,
						flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))

				go func(flight types.FlightInfo) {
					time.Sleep(5 * time.Minute)

					flight.FailCount++
					e.UFlightScheduler.Submit(flight)
				}(flight)
			}
		}
	}()
}

// 携程的爬取逻辑总控
func (e RealTimeEngine) CreateCWorker(in chan types.FlightInfo, out chan types.ParseResult) {
	go func() {
		for {
			flight := <-in

			// 如果总的抓取次数大于阈值，则换用飞常准查询
			if flight.FetchCount >= 40 {
				flight.FetchCount = 0
				e.VFlightScheduler.Submit(flight)

				continue
			}
			// 连续出错2次，该数据源无效，需要更换数据源
			if flight.FailCount >= 2 {
				utils.AppendToFile(logs.CLogFile,
					fmt.Sprintf(">=1:[%s]change source for entry [%s:%s %d/%d]\n",
						time.Now().Format("2006-01-02 15:04:05"),
						flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))

				// note: change data source
				flight.FailCount = 0
				e.VFlightScheduler.Submit(flight)

				continue
			}

			flight.FetchCount++

			// 携程 数据源
			request := ctrip.DetailRequest(flight)
			result, err := fetcher.FetchRequest(request, e.CRateLimiter)
			if err != nil {
				// 获取程序报错
				e.CRateLimiter.Slower()
				utils.AppendToFile(logs.CLogFile,
					fmt.Sprintf("==1:[%s]fetch worker error:%q [%s:%s %d/%d]\n",
						time.Now().Format("2006-01-02 15:04:05"), err,
						flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))

				go func(flight types.FlightInfo) {
					time.Sleep(5 * time.Minute)

					// 失败次数累加
					flight.FailCount++
					e.CFlightScheduler.Submit(flight)
				}(flight)

				continue
			}

			if valid, state := ResultIsValid(result); valid {
				// 航班状态正常
				out <- result
				if ResultIsFinish(result) == false {
					// 航班没有结束
					utils.AppendToFile(logs.CLogFile,
						fmt.Sprintf("==2:[%s]fetch success entry [%s:%s %d/%d]\n",
							time.Now().Format("2006-01-02 15:04:05"),
							flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))

					go func(flight types.FlightInfo) {
						time.Sleep(5 * time.Minute)

						flight.FailCount = 0
						e.CFlightScheduler.Submit(flight)
					}(flight)
				} else {
					// 航班结束
					utils.AppendToFile(logs.CLogFile,
						fmt.Sprintf("==3:[%s]fetch finish entry [%s:%s %d/%d]\n",
							time.Now().Format("2006-01-02 15:04:05"),
							flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))
				}
			} else {
				// 航班状态异常
				utils.AppendToFile(logs.CLogFile,
					fmt.Sprintf("==4:[%s]fetch invalid status entry:%q [%s:%s %d/%d]\n",
						time.Now().Format("2006-01-02 15:04:05"), state,
						flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))

				go func(flight types.FlightInfo) {
					time.Sleep(5 * time.Minute)

					flight.FailCount++
					e.CFlightScheduler.Submit(flight)
				}(flight)
			}
		}
	}()
}

// 飞常准的爬取逻辑总控
func (e RealTimeEngine) CreateVWorker(in chan types.FlightInfo, out chan types.ParseResult) {
	go func() {
		for {
			flight := <-in

			if flight.FailCount >= 5 {
				utils.AppendToFile(logs.VLogFile,
					fmt.Sprintf(">=1:[%s]change source for entry [%s:%s %d/%d]\n",
						time.Now().Format("2006-01-02 15:04:05"),
						flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))

				// 更换数据源
				flight.FailCount = 0
				e.UFlightScheduler.Submit(flight)
				e.CFlightScheduler.Submit(flight)

				continue
			}

			flight.FetchCount++

			// 数据源:飞常准
			request := veryzhun.DetailRequest(flight)
			result, err := fetcher.FetchRequestWithProxy(request, e.ProxyPool, e.VRateLimiter)
			if err != nil {
				// 获取程序报错
				if err == veryParser.ErrNoData {
					// 无此航班，删除数据库中的相关航班
					err = store.RemoveFlight(flight)
					if err != nil {
						utils.AppendToFile(logs.VLogFile,
							fmt.Sprintf("==0:[%s]remove flight error:%q [%s:%s %d/%d]\n",
								time.Now().Format("2006-01-02 15:04:05"), err,
								flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))
					} else {
						utils.AppendToFile(logs.VLogFile,
							fmt.Sprintf("==0:[%s]remove flight success [%s:%s %d/%d]\n",
								time.Now().Format("2006-01-02 15:04:05"),
								flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))
					}
				} else {
					go func(flight types.FlightInfo) {
						utils.AppendToFile(logs.VLogFile,
							fmt.Sprintf("==1:[%s]fetch worker error:%q [%s:%s %d/%d]\n",
								time.Now().Format("2006-01-02 15:04:05"), err,
								flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))

						time.Sleep(5 * time.Minute)
						flight.FailCount++
						e.VFlightScheduler.Submit(flight)
					}(flight)
				}
				continue
			}

			if valid, state := ResultIsValid(result); valid {
				// 航班状态正常
				out <- result

				if ResultIsFinish(result) == false {
					// 航班没有结束
					go func(flight types.FlightInfo) {
						utils.AppendToFile(logs.VLogFile,
							fmt.Sprintf("==2:[%s]fetch success entry [%s:%s %d/%d]\n",
								time.Now().Format("2006-01-02 15:04:05"),
								flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))

						time.Sleep(5 * time.Minute)
						flight.FailCount = 0
						e.VFlightScheduler.Submit(flight)
					}(flight)
				} else {
					// 航班结束
					utils.AppendToFile(logs.VLogFile,
						fmt.Sprintf("==3:[%s]fetch finish entry [%s:%s %d/%d]\n",
							time.Now().Format("2006-01-02 15:04:05"),
							flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))
				}
			} else {
				// 航班状态异常
				go func(flight types.FlightInfo) {
					utils.AppendToFile(logs.VLogFile,
						fmt.Sprintf("==4:[%s]fetch invalid status entry:%q [%s:%s %d/%d]\n",
							time.Now().Format("2006-01-02 15:04:05"), state,
							flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))

					time.Sleep(5 * time.Minute)
					flight.FailCount++
					e.VFlightScheduler.Submit(flight)
				}(flight)
			}
		}
	}()
}

// 对抓取的结果进行判定，决定这个结果是否是正常结果
// 如果是非正常结果，则会重新抓取
func ResultIsValid(result types.ParseResult) (bool, string) {
	if len(result.Items) == 0 {
		return false, ""
	}
	state := ""

	switch result.Request.Source {
	case "umetrip":
		for _, item := range result.Items {
			flightItem := item.(umetripParser.FlightDetailData)
			state = flightItem.FlightState
			if StateIsValid(flightItem.FlightState) {
				return true, state
			}
		}
	case "ctrip":
		for _, item := range result.Items {
			flightItem := item.(ctripParser.FlightDetailData)
			state = flightItem.FlightState
			if StateIsValid(flightItem.FlightState) {
				return true, state
			}
		}
	case "veryzhun":
		for _, item := range result.Items {
			flightItem := item.(veryParser.FlightDetailData)
			state = flightItem.FlightState
			if StateIsValid(flightItem.FlightState) {
				return true, state
			}
		}
	default:
		panic(fmt.Sprintf("request source not valid:%q", result.Request.Source))
	}

	return false, state
}

func ResultIsFinish(result types.ParseResult) bool {
	switch result.Request.Source {
	case "umetrip":
		for _, item := range result.Items {
			flightItem := item.(umetripParser.FlightDetailData)
			if StateIsFinish(flightItem.FlightState) == false {
				return false
			}
		}
	case "ctrip":
		for _, item := range result.Items {
			flightItem := item.(ctripParser.FlightDetailData)
			if StateIsFinish(flightItem.FlightState) == false {
				return false
			}
		}
	case "veryzhun":
		for _, item := range result.Items {
			flightItem := item.(veryParser.FlightDetailData)
			if StateIsFinish(flightItem.FlightState) == false {
				return false
			}
		}
	default:
		panic(fmt.Sprintf("request source not valid:%q", result.Request.Source))
	}

	return true
}

func StateIsValid(state string) bool {
	switch state {
	// 正常状态
	case "到达", "取消", "备降", "返航", "返航取消", "备降取消":
		return true
		// 携程的状态
	case "计划", "起飞", "延误", "可能延误", "可能取消", "可能备降", "可能返航", "开舱", "预警":
		return true
		// 飞常准的状态
	case "备降起飞", "备降到达", "返航起飞", "返航到达", "即将到达", "开始上升", "正在上升", "开始下降", "正在下降", "开始巡航", "盘旋过", "正在盘旋", "提前取消":
		return true

		// 不正常状态
	case "", "暂无":
		return false
	default:
		utils.AppendToFile(logs.InfoLogFile, fmt.Sprintf("invalid state: %q\n", state))
		return false
	}
}

func StateIsFinish(state string) bool {
	// note: 备降 不再作为最终状态，因为有可能还会出现 备降取消 和 备降到达
	switch state {
	case "到达", "取消", "备降到达", "备降取消", "返航", "返航到达", "返航取消", "提前取消":
		return true
	}

	return false
}
