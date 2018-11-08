package main

import (
	"database/sql"
	"fmt"
	"time"

	"sync"

	veryParser "github.com/champkeh/crawler/source/veryzhun/parser"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/fetcher"

	"github.com/champkeh/crawler/proxy/pool"
	"github.com/champkeh/crawler/ratelimiter"
	"github.com/champkeh/crawler/scheduler"
	"github.com/champkeh/crawler/source/veryzhun"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/utils"
	_ "github.com/denisenkom/go-mssqldb"
)

// SubscribeEngine
//
// 这个引擎用于实时爬取 [FlightBaseData].[dbo].[TodoSearchFlight] 订阅表中的航班
// 爬取结果保存在 [FlightBaseData].[dbo].[SearchFlightResult] 表
// 爬取数据来源于飞常准
type SubscribeEngine struct {
	// 飞常准 veryzhun 的航班容器的调度器
	VFlightScheduler types.FlightScheduler
	// 频率限制器
	VRateLimiter types.RateLimiter
	// 代理ip池
	ProxyPool *pool.LocalProxyPool

	WorkerCount int
	mutex       sync.Mutex
}

// DefaultSubscribeEngine
//
// 引擎的默认配置
var DefaultSubscribeEngine = SubscribeEngine{
	VFlightScheduler: &scheduler.SimpleFlightScheduler{},
	VRateLimiter:     ratelimiter.NewSimpleRateLimiterFull(20, 5000, 50),
	ProxyPool:        &pool.LocalProxyPool{},
	WorkerCount:      100,
}

var T = time.Now()

var (
	InfoLogFile = "subscribe.info.log"
	SqlLogFile  = "subscribe.save.log"
)

func main() {
	DefaultSubscribeEngine.Run()
}

// Run
//
// 启动实时抓取引擎
func (e SubscribeEngine) Run() {

	// 确保日志文件存在
	utils.MustExist(InfoLogFile)
	utils.MustExist(SqlLogFile)

	// 航班容器
	vFlightChannel := make(chan types.FlightInfo, 3000)

	// 从航班订阅表中拉取未来1天起飞的航班，放在 flightChannel 容器中
	PullTaskDataFromDB(vFlightChannel)

	go func() {
		// 然后，每隔5分钟拉取一次
		ticker := time.NewTicker(5 * time.Minute)
		for {
			select {
			case <-ticker.C:
				// 每次拉取数据，都重置时间计数器
				T = time.Now()

				// 从航班订阅表中拉取未来1天起飞的航班，放在 flightChannel 容器中
				PullTaskDataFromDB(vFlightChannel)
			}
		}
	}()

	e.VFlightScheduler.ConfigureFlightChan(vFlightChannel)

	// configure scheduler's out channel, has 100 space buffer channel
	out := make(chan types.ParseResult, 1000)

	// pipe channel
	for i := 0; i < e.WorkerCount; i++ {
		e.CreateVWorker(vFlightChannel, out)
	}

	// run the rate-limiter
	go e.VRateLimiter.Run()
	go e.ProxyPool.Start(1000, "proxyip_verify", SqlLogFile)

	counter := 0
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case result := <-out:
			go SaveParseResultFromVeryzhun(result)
			counter++
			counter %= 1000

		case <-ticker.C:
			fmt.Printf("\r[%s %d] [VCh:%d Proxy:%d VR:%.2f]", time.Since(T), counter, len(vFlightChannel),
				e.ProxyPool.Count(), e.VRateLimiter.QPS())
		}
	}
}

// 飞常准的爬取逻辑总控
func (e SubscribeEngine) CreateVWorker(in chan types.FlightInfo, out chan types.ParseResult) {
	go func() {
		for {
			flight := <-in

			if flight.FlightDate <= time.Now().AddDate(0, 0, -2).Format("2006-01-02") {
				// 停止跟踪
				utils.AppendToFile(InfoLogFile,
					fmt.Sprintf(">=2:[%s]FlightDate are too early, skip this entry [%s:%s %d/%d]\n",
						time.Now().Format("2006-01-02 15:04:05"),
						flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))
				continue
			} else if flight.FlightDate <= time.Now().AddDate(0, 0, -1).Format("2006-01-02") {
				// 报警
				utils.AppendToFile(InfoLogFile,
					fmt.Sprintf(">=1:[%s]fetch this task fail [%s:%s %d/%d]\n",
						time.Now().Format("2006-01-02 15:04:05"),
						flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))
				//todo: 发邮件
			}

			flight.FetchCount++

			// 数据源:飞常准
			request := veryzhun.DetailRequest(flight)
			result, err := fetcher.FetchRequestWithProxy(request, e.ProxyPool, e.VRateLimiter)
			if err != nil {
				// 获取程序报错
				if err == veryParser.ErrNoData {
					utils.AppendToFile(InfoLogFile,
						fmt.Sprintf("==0:[%s]flight not exist [%s:%s %d/%d]\n",
							time.Now().Format("2006-01-02 15:04:05"),
							flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))
				} else {
					go func(flight types.FlightInfo) {
						utils.AppendToFile(InfoLogFile,
							fmt.Sprintf("==1:[%s]fetch worker error:%q [%s:%s %d/%d]\n",
								time.Now().Format("2006-01-02 15:04:05"), err,
								flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))

						time.Sleep(1 * time.Minute)
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
				} else {
					// 航班结束
					// 修改 IsCompleted 字段
					err = UpdateIsCompletedField(result.Request.RawParam.Fno, result.Request.RawParam.Date)
					if err == nil {
						utils.AppendToFile(InfoLogFile,
							fmt.Sprintf("==3:[%s]fetch finish entry [%s:%s %d/%d]\n",
								time.Now().Format("2006-01-02 15:04:05"),
								flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))
					}
				}
			} else {
				// 航班状态异常
				go func(flight types.FlightInfo) {
					utils.AppendToFile(InfoLogFile,
						fmt.Sprintf("==4:[%s]fetch invalid status entry:%q [%s:%s %d/%d]\n",
							time.Now().Format("2006-01-02 15:04:05"), state,
							flight.FlightDate, flight.FlightNo, flight.FailCount, flight.FetchCount))

					time.Sleep(1 * time.Minute)
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

	for _, item := range result.Items {
		flightItem := item.(veryParser.FlightDetailData)
		state = flightItem.FlightState
		if StateIsValid(flightItem.FlightState) {
			return true, state
		}
	}

	return false, state
}

func ResultIsFinish(result types.ParseResult) bool {
	for _, item := range result.Items {
		flightItem := item.(veryParser.FlightDetailData)
		if StateIsFinish(flightItem.FlightState) == false {
			return false
		}
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
		utils.AppendToFile(InfoLogFile, fmt.Sprintf("invalid state: %q\n", state))
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

// 从航班订阅表 TodoSearchFlight 表中拉取未来1天起飞的航班
func PullTaskDataFromDB(container chan types.FlightInfo) {

	// 连接到 FlightBaseData 数据库
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s", config.SqlUser, config.SqlPass, config.SqlHost,
		"FlightBaseData")
	db, err := sql.Open("sqlserver", connstr)
	if err != nil {
		panic(err)
	}

	// 拉取截止到明天的所有未完成航班
	date := time.Now().AddDate(0, 0, 1).Format("2006-01-02")
	query := fmt.Sprintf("select distinct FlightDate,FlightNo from [dbo].[TodoSearchFlight] "+
		"where IsCompleted=0 and FlightDate<='%s'", date)

	go func() {
		rows, err := db.Query(query)
		if err != nil {
			utils.AppendToFile(SqlLogFile,
				fmt.Sprintf("[%s]:query from TodoSearchFlight table error:%q\n",
					time.Now().Format("2006-01-02 15:04:05"), err))
			panic(err)
		}
		defer db.Close()
		defer rows.Close()

		var flight types.FlightInfo
		count := 0
		for rows.Next() {
			err := rows.Scan(&flight.FlightDate, &flight.FlightNo)
			if err != nil {
				utils.AppendToFile(SqlLogFile,
					fmt.Sprintf("[%s]:scan TodoSearchFlight error:%q\n",
						time.Now().Format("2006-01-02 15:04:05"), err))
				continue
			}
			container <- flight
			count++
		}
		utils.AppendToFile(SqlLogFile,
			fmt.Sprintf("[%s]:pull %d flight from TodoSearchFlight table (%s)\n",
				time.Now().Format("2006-01-02 15:04:05"), count, query))
	}()
}

// 保存从飞常准(veryzhun)抓取的 ParseResult
func SaveParseResultFromVeryzhun(result types.ParseResult) {
	for _, item := range result.Items {
		data := item.(veryParser.FlightDetailData)
		UpdateFlightItemFromVeryzhun(data)
	}
}

// 保存从飞常准(veryzhun)抓取的 FlightDetailData
func UpdateFlightItemFromVeryzhun(data veryParser.FlightDetailData) {

	// 连接到 FlightBaseData 数据库
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s", config.SqlUser, config.SqlPass, config.SqlHost,
		"FlightBaseData")
	db, err := sql.Open("sqlserver", connstr)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// #1. 修正时间字段
	//
	// 如果没有起飞，则实际起飞时间字段 FlightDeptimeDate 为空
	// 如果没有到达，则实际到达时间字段 FlightArrtimeDate 为空
	//
	// 飞常准抓取的时间字段格式为: 2018-10-24 18:15:00
	depPlanTime := data.FlightDeptimePlanDate
	depActualTime := data.FlightDeptimeDate
	depReadyTime := data.FlightDeptimeReadyDate
	arrPlanTime := data.FlightArrtimePlanDate
	arrActualTime := data.FlightArrtimeDate
	arrReadyTime := data.FlightArrtimeReadyDate

	if depActualTime == "" {
		depActualTime = "1990-01-01 00:00:00"
	}
	if depReadyTime == "" {
		depReadyTime = "1990-01-01 00:00:00"
	}
	if arrActualTime == "" {
		arrActualTime = "1990-01-01 00:00:00"
	}
	if arrReadyTime == "" {
		arrReadyTime = "1990-01-01 00:00:00"
	}

	// #2. 获取数据库中该航段的最新状态
	id := 0
	err = db.QueryRow(fmt.Sprintf(
		"select top 1 id from [dbo].[SearchFlightResult]"+
			" where FlightDate='%s'"+
			" and FlightNo='%s'"+
			" and DepCode='%s'"+
			" and ArrCode='%s'"+
			" order by DtCreate desc",
		data.FlightDate, data.FlightNo, data.FlightDepcode, data.FlightArrcode)).Scan(&id)
	if id == 0 {
		// 没有记录，则插入新的记录
		_, err = db.Exec(fmt.Sprintf("insert into [dbo].[SearchFlightResult]"+
			"(FlightDate,FlightNo,FlightCompany,FlightState,"+
			"DepCity,DepCode,DepAirport,"+
			"DeptimeDate,DeptimePlanDate,DeptimeReadyDate,"+
			"ArrCity,ArrCode,ArrAirport,"+
			"ArrtimeDate,ArrtimePlanDate,ArrtimeReadyDate,"+
			"DepWeather,ArrWeather,DtCreate)"+
			" values"+
			"('%s','%s','%s','%s',"+
			"'%s','%s','%s',"+
			"'%s','%s','%s',"+
			"'%s','%s','%s',"+
			"'%s','%s','%s',"+
			"'%s','%s','%s')",
			data.FlightDate,
			data.FlightNo,
			data.FlightCompany,
			data.FlightState,
			data.FlightDep,
			data.FlightDepcode,
			data.FlightDepAirport,
			depActualTime,
			depPlanTime,
			depReadyTime,
			data.FlightArr,
			data.FlightArrcode,
			data.FlightArrAirport,
			arrActualTime,
			arrPlanTime,
			arrReadyTime,
			data.DepWeather,
			data.ArrWeather,
			time.Now().Format("2006-01-02 15:04:05")))
		if err != nil {
			utils.AppendToFile(SqlLogFile,
				fmt.Sprintf("[%s]:insert into SearchFlightResult table error:%q [%s:%s:%s:%s]\n",
					time.Now().Format("2006-01-02 15:04:05"), err,
					data.FlightDate, data.FlightNo, data.FlightDepcode, data.FlightArrcode))
		}
	} else {
		// 更新现有记录
		_, err = db.Exec(fmt.Sprintf("update [dbo].[SearchFlightResult]"+
			" set"+
			" FlightCompany='%s',"+
			" FlightState='%s',"+
			" DeptimeDate='%s',"+
			" DeptimePlanDate='%s',"+
			" DeptimeReadyDate='%s',"+
			" ArrtimeDate='%s',"+
			" ArrtimePlanDate='%s',"+
			" ArrtimeReadyDate='%s',"+
			" DepWeather='%s',"+
			" ArrWeather='%s',"+
			" DtCreate='%s'"+
			" where id=%d",
			data.FlightCompany,
			data.FlightState,
			depActualTime,
			depPlanTime,
			depReadyTime,
			arrActualTime,
			arrPlanTime,
			arrReadyTime,
			data.DepWeather,
			data.ArrWeather,
			time.Now().Format("2006-01-02 15:04:05"),
			id))
		if err != nil {
			utils.AppendToFile(SqlLogFile,
				fmt.Sprintf("[%s]:update SearchFlightResult item error:%q [%s:%s:%s:%s]\n",
					time.Now().Format("2006-01-02 15:04:05"), err,
					data.FlightDate, data.FlightNo, data.FlightDepcode, data.FlightArrcode))
		}
	}
}

// 更新TodoSearchFlight表中的IsCompleted字段为1
func UpdateIsCompletedField(fno, fdate string) error {
	// 连接到 FlightBaseData 数据库
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s", config.SqlUser, config.SqlPass, config.SqlHost,
		"FlightBaseData")
	db, err := sql.Open("sqlserver", connstr)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	result, err := db.Exec(fmt.Sprintf("update [dbo].[TodoSearchFlight]"+
		" set IsCompleted=1"+
		" where FlightNo='%s' and FlightDate='%s'", fno, fdate))
	if err != nil {
		utils.AppendToFile(SqlLogFile,
			fmt.Sprintf("[%s]:update IsCompleted field error:%q [%s:%s]\n",
				time.Now().Format("2006-01-02 15:04:05"), err, fdate, fno))
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		utils.AppendToFile(SqlLogFile,
			fmt.Sprintf("[%s]:update IsCompleted field rowsAffected error:%q [%s:%s]\n",
				time.Now().Format("2006-01-02 15:04:05"), err, fdate, fno))
		return err
	}
	utils.AppendToFile(SqlLogFile,
		fmt.Sprintf("[%s]:update IsCompleted field rowsAffected:%d [%s:%s]\n",
			time.Now().Format("2006-01-02 15:04:05"), rowsAffected, fdate, fno))
	return nil
}
