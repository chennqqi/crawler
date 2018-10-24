package persist

import (
	"database/sql"
	"fmt"

	"time"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/logs"
	ctripParser "github.com/champkeh/crawler/source/ctrip/parser"
	umetripParser "github.com/champkeh/crawler/source/umetrip/parser"
	veryParser "github.com/champkeh/crawler/source/veryzhun/parser"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/utils"
	_ "github.com/denisenkom/go-mssqldb"
)

func init() {
	var err error

	// 连接到 FlightData 数据库
	connstr := fmt.Sprintf("sqlserver://%s:%s@%s?database=%s", config.SqlUser, config.SqlPass, config.SqlHost,
		"FlightData")
	db, err = sql.Open("sqlserver", connstr)
	if err != nil {
		panic(err)
	}
}

type simpleFlightDetailData struct {
	FlightNo         string
	Date             string
	DepCode          string
	ArrCode          string
	FlightState      string
	DepPlanTime      string
	DepActualTime    string
	ArrPlanTime      string
	ArrActualTime    string
	PreFlightNo      string
	PreFlightState   string
	CheckinCounter   string
	BoardGate        string
	BaggageTurntable string
}

func SaveToRealTime(result types.ParseResult) {
	switch result.Request.Source {
	case "umetrip":
		SaveParseResultFromUmetrip(result)
	case "ctrip":
		SaveParseResultFromCtrip(result)
	case "veryzhun":
		SaveParseResultFromVeryzhun(result)
	default:
		panic(fmt.Sprintf("invalid request source at save:%q", result.Request.Source))
	}
}

// 航联纵横(umetrip)的航班有更新
func UmetripHasChange(newData umetripParser.FlightDetailData, oldData simpleFlightDetailData) bool {

	// newData中的时间格式为: 2018-10-01 10:00
	if oldData.DepPlanTime[0:16] != newData.DepPlanTime || oldData.DepActualTime[0:16] != newData.DepActualTime ||
		oldData.ArrPlanTime[0:16] != newData.ArrPlanTime || oldData.ArrActualTime[0:16] != newData.ArrActualTime {
		// 时间值有更新
		return true
	}

	// 比较航班状态
	if oldData.FlightState != newData.FlightState {
		// 航班状态有更新
		return true
	}

	// 比较前序航班状态
	if oldData.PreFlightNo != newData.PreFlightNo || oldData.PreFlightState != newData.PreFlightState {
		// 前序航班状态有更新
		return true
	}
	return false
}

// 保存从航联纵横(umetrip)抓取的 FlightDetailData
func UpdateFlightItemFromUmetrip(data umetripParser.FlightDetailData) {

	// #1. 将code解析为time
	//
	// 从航联纵横获取的时间字段均为 time(00:00)，不包含日期部分
	// 并且time还是经过某种编码变为一个字符串，比如:QKGxTjvyrbn043SGyMiy/w==
	// 所以，此处需要进行解码
	depPlanTime := utils.Code2Time(data.DepPlanTime)
	depActualTime := utils.Code2Time(data.DepActualTime)
	arrPlanTime := utils.Code2Time(data.ArrPlanTime)
	arrActualTime := utils.Code2Time(data.ArrActualTime)

	// #2. 将time转为datetime
	//
	// 根据抓取到的航班日期和对应的time，计算出一个合理的 datetime
	// 格式为: 2018-10-01 10:00
	data.DepPlanTime = utils.TimeToDatetime(data.FlightDate, depPlanTime, depPlanTime)
	data.DepActualTime = utils.TimeToDatetime(data.FlightDate, depPlanTime, depActualTime)
	data.ArrPlanTime = utils.TimeToDatetime(data.FlightDate, depPlanTime, arrPlanTime)
	data.ArrActualTime = utils.TimeToDatetime(data.FlightDate, depPlanTime, arrActualTime)

	// #3. 检查数据库中的状态是否为最新状态
	// 获取数据库中该航班的最新状态并进行比较
	var dbFlightData simpleFlightDetailData
	err := db.QueryRow(fmt.Sprintf(
		"select top 1 "+
			"flightNo,date,depCode,arrCode,flightState,"+
			"depPlanTime,depActualTime,arrPlanTime,arrActualTime,"+
			"preFlightNo,preFlightState "+
			"from [dbo].[RealTime] "+
			"where flightNo='%s' and date='%s' and depCode='%s' and arrCode='%s' "+
			"order by updateAt desc",
		data.FlightNo, data.FlightDate, data.DepCode, data.ArrCode)).Scan(
		&dbFlightData.FlightNo,
		&dbFlightData.Date,
		&dbFlightData.DepCode,
		&dbFlightData.ArrCode,
		&dbFlightData.FlightState,
		&dbFlightData.DepPlanTime,
		&dbFlightData.DepActualTime,
		&dbFlightData.ArrPlanTime,
		&dbFlightData.ArrActualTime,
		&dbFlightData.PreFlightNo,
		&dbFlightData.PreFlightState)

	if err == sql.ErrNoRows {

		// 数据库不存在: 只有当天及之后的航班，才进行插入操作
		// 因为之前的不存在的航班，可能是已经归档了
		if data.FlightDate >= time.Now().Format("2006-01-02") {
			// 插入
			_, err = db.Exec(fmt.Sprintf("insert into [dbo].[RealTime]"+
				"(flightNo,date,depCode,arrCode,depCity,arrCity,flightState,"+
				"depPlanTime,depExpTime,depActualTime,arrPlanTime,arrExpTime,arrActualTime,"+
				"mileage,duration,age,"+
				"preFlightNo,preFlightState,preFlightDepCode,preFlightArrCode,"+
				"depWeather,arrWeather,depFlow,arrFlow,"+
				"updateAt,"+
				"source)"+
				" values ("+
				"'%s','%s','%s','%s','%s','%s','%s',"+
				"'%s','%s','%s','%s','%s','%s',"+
				"'%s','%s','%s',"+
				"'%s','%s','%s','%s',"+
				"'%s','%s','%s','%s',"+
				"'%s','%s')",
				data.FlightNo,
				data.FlightDate,
				data.DepCode,
				data.ArrCode,
				data.DepCity,
				data.ArrCity,
				data.FlightState,
				data.DepPlanTime,
				data.DepActualTime,
				data.DepActualTime,
				data.ArrPlanTime,
				data.ArrActualTime,
				data.ArrActualTime,
				data.Mileage,
				data.Duration,
				data.Age,
				data.PreFlightNo,
				data.PreFlightState,
				data.PreFlightDepCode,
				data.PreFlightArrCode,
				data.DepWeather,
				data.ArrWeather,
				data.DepFlow,
				data.ArrFlow,
				time.Now().Format("2006-01-02 15:04:05"),
				"umetrip"))
			if err != nil {
				utils.AppendToFile(logs.SaveInfoLogFile,
					fmt.Sprintf("[umetrip %s]:insert into RealTime table error:%q [%s:%s:%s:%s]\n",
						time.Now().Format("2006-01-02 15:04:05"), err,
						data.FlightDate, data.FlightNo, data.DepCode, data.ArrCode))
			}
		} else {
			// 从航联纵横抓取到的航班，被丢弃了
			utils.AppendToFile(logs.SaveInfoLogFile,
				fmt.Sprintf("[umetrip %s]:skip early entry from umetrip [%s:%s:%s:%s]\n",
					time.Now().Format("2006-01-02 15:04:05"),
					data.FlightDate, data.FlightNo, data.DepCode, data.ArrCode))
		}
	} else if err == nil {
		// 检查数据库中的航班是否为最新状态
		if UmetripHasChange(data, dbFlightData) == false {
			// 状态没有更新，则不需要保存
			return
		}

		// 判断状态是否合理
		// 计划<起飞<到达
		// 预警<计划<起飞<到达
		//todo: 此处需要检查航班状态的出现顺序是否正常
		//if data.FlightState == "暂无" && dbFlightData.FlightState != "暂无" {
		//	utils.AppendToFile(logs.SaveInfoLogFile,
		//		fmt.Sprintf("[umetrip %s]:skip early entry from umetrip [%s:%s:%s:%s]\n",
		//			time.Now().Format("2006-01-02 15:04:05"),
		//			data.FlightDate, data.FlightNo, data.DepCode, data.ArrCode))
		//	return
		//}

		result, err := db.Exec(fmt.Sprintf("update [dbo].[RealTime]"+
			" set"+
			" depCity='%s',"+
			" arrCity='%s',"+
			" flightState='%s',"+
			" depPlanTime='%s',"+
			" depExpTime='%s',"+
			" depActualTime='%s',"+
			" arrPlanTime='%s',"+
			" arrExpTime='%s',"+
			" arrActualTime='%s',"+
			" mileage='%s',"+
			" duration='%s',"+
			" age='%s',"+
			" preFlightNo='%s',"+
			" preFlightState='%s',"+
			" preFlightDepCode='%s',"+
			" preFlightArrCode='%s',"+
			" depWeather='%s',"+
			" arrWeather='%s',"+
			" depFlow='%s',"+
			" arrFlow='%s',"+
			" source='%s',"+
			" updateAt='%s'"+
			" where flightNo='%s' and date='%s' and depCode='%s' and arrCode='%s'",
			data.DepCity,
			data.ArrCity,
			data.FlightState,
			data.DepPlanTime,
			data.DepActualTime,
			data.DepActualTime,
			data.ArrPlanTime,
			data.ArrActualTime,
			data.ArrActualTime,
			data.Mileage,
			data.Duration,
			data.Age,
			data.PreFlightNo,
			data.PreFlightState,
			data.PreFlightDepCode,
			data.PreFlightArrCode,
			data.DepWeather,
			data.ArrWeather,
			data.DepFlow,
			data.ArrFlow,
			"umetrip",
			time.Now().Format("2006-01-02 15:04:05"),
			data.FlightNo, data.FlightDate, data.DepCode, data.ArrCode))
		if err != nil {
			utils.AppendToFile(logs.SaveInfoLogFile,
				fmt.Sprintf("[umetrip %s]:update flight item error:%q [%s:%s:%s:%s]\n",
					time.Now().Format("2006-01-02 15:04:05"), err,
					data.FlightDate, data.FlightNo, data.DepCode, data.ArrCode))
			return
		}

		// 检查更新了几条记录
		n, err := result.RowsAffected()
		if err != nil {
			utils.AppendToFile(logs.SaveInfoLogFile,
				fmt.Sprintf("[umetrip %s]:check RowsAffected error:%q [%s:%s:%s:%s]\n",
					time.Now().Format("2006-01-02 15:04:05"), err,
					data.FlightDate, data.FlightNo, data.DepCode, data.ArrCode))
			return
		} else if n != 1 {
			utils.AppendToFile(logs.SaveInfoLogFile,
				fmt.Sprintf("[umetrip %s]:check RowsAffected fail:%d [%s:%s:%s:%s]\n",
					time.Now().Format("2006-01-02 15:04:05"), n,
					data.FlightDate, data.FlightNo, data.DepCode, data.ArrCode))
			return
		}

		// 更新抓取时间字段
		if data.FlightState == "起飞" && dbFlightData.FlightState != "起飞" {
			_, err = db.Exec(fmt.Sprintf("update [dbo].[RealTime]"+
				" set"+
				" depAt='%s'"+
				" where flightNo='%s' and date='%s' and depCode='%s' and arrCode='%s'",
				time.Now().Format("2006-01-02 15:04:05"),
				data.FlightNo, data.FlightDate, data.DepCode, data.ArrCode))
			if err != nil {
				utils.AppendToFile(logs.SaveInfoLogFile,
					fmt.Sprintf("[umetrip %s]:update depAt field error:%q [%s:%s:%s:%s]\n",
						time.Now().Format("2006-01-02 15:04:05"), err,
						data.FlightDate, data.FlightNo, data.DepCode, data.ArrCode))
			}
		} else if data.FlightState == "到达" && dbFlightData.FlightState != "到达" {
			_, err = db.Exec(fmt.Sprintf("update [dbo].[RealTime]"+
				" set"+
				" arrAt='%s'"+
				" where flightNo='%s' and date='%s' and depCode='%s' and arrCode='%s'",
				time.Now().Format("2006-01-02 15:04:05"),
				data.FlightNo, data.FlightDate, data.DepCode, data.ArrCode))
			if err != nil {
				utils.AppendToFile(logs.SaveInfoLogFile,
					fmt.Sprintf("[umetrip %s]:update arrAt field error:%q [%s:%s:%s:%s]\n",
						time.Now().Format("2006-01-02 15:04:05"), err,
						data.FlightDate, data.FlightNo, data.DepCode, data.ArrCode))
			}
		}
	} else {
		utils.AppendToFile(logs.SaveInfoLogFile,
			fmt.Sprintf("[umetrip %s]:db.QueryRow error:%q [%s:%s:%s:%s]\n",
				time.Now().Format("2006-01-02 15:04:05"), err,
				data.FlightDate, data.FlightNo, data.DepCode, data.ArrCode))
	}
}

// 保存从航联纵横(umetrip)抓取的 ParseResult
func SaveParseResultFromUmetrip(result types.ParseResult) {
	for _, item := range result.Items {
		data := item.(umetripParser.FlightDetailData)
		UpdateFlightItemFromUmetrip(data)
	}
}

// 携程(ctrip)的航班有更新
func CtripHasChange(newData ctripParser.FlightDetailData, oldData simpleFlightDetailData) bool {

	// newData中的时间格式为: 2018-10-01 10:00
	if oldData.DepPlanTime[0:16] != newData.DepPlanTime || oldData.DepActualTime[0:16] != newData.DepActualTime ||
		oldData.ArrPlanTime[0:16] != newData.ArrPlanTime || oldData.ArrActualTime[0:16] != newData.ArrActualTime {
		return true
	}

	// 比较航班状态
	if oldData.FlightState != newData.FlightState {
		return true
	}

	// 比较值机柜台信息
	if oldData.CheckinCounter != newData.CheckinCounter || oldData.BoardGate != newData.BoardGate ||
		oldData.BaggageTurntable != newData.BaggageTurntable {
		return true
	}
	return false
}

// 保存从携程(ctrip)抓取的 FlightDetailData
func UpdateFlightItemFromCtrip(data ctripParser.FlightDetailData) {

	// #1. 将time转为datetime
	//
	// 从携程获取的时间字段为 time(08:00)，不包含日期部分
	// 根据抓取到的航班日期和对应的time，计算出一个合理的 datetime
	// 格式为: 2018-10-01 10:00
	depPlanTime := data.DepPlanTime
	depActualTime := data.DepActualTime
	arrPlanTime := data.ArrPlanTime
	arrActualTime := data.ArrActualTime

	data.DepPlanTime = utils.TimeToDatetime(data.FlightDate, depPlanTime, depPlanTime)
	data.DepActualTime = utils.TimeToDatetime(data.FlightDate, depPlanTime, depActualTime)
	data.ArrPlanTime = utils.TimeToDatetime(data.FlightDate, depPlanTime, arrPlanTime)
	data.ArrActualTime = utils.TimeToDatetime(data.FlightDate, depPlanTime, arrActualTime)

	// #2. 获取数据库中该航班的最新状态并进行比较
	var dbFlightData simpleFlightDetailData
	err := db.QueryRow(fmt.Sprintf(
		"select top 1 "+
			"flightNo,date,depCode,arrCode,flightState,"+
			"depPlanTime,depActualTime,arrPlanTime,arrActualTime,"+
			"checkinCounter,boardGate,baggageTurntable "+
			"from [dbo].[RealTime] "+
			"where flightNo='%s' and date='%s' and depPlanTime='%s' and arrPlanTime='%s' "+
			"order by updateAt desc",
		data.FlightNo, data.FlightDate, data.DepPlanTime, data.ArrPlanTime)).Scan(
		&dbFlightData.FlightNo,
		&dbFlightData.Date,
		&dbFlightData.DepCode,
		&dbFlightData.ArrCode,
		&dbFlightData.FlightState,
		&dbFlightData.DepPlanTime,
		&dbFlightData.DepActualTime,
		&dbFlightData.ArrPlanTime,
		&dbFlightData.ArrActualTime,
		&dbFlightData.CheckinCounter,
		&dbFlightData.BoardGate,
		&dbFlightData.BaggageTurntable)

	if err == sql.ErrNoRows {
		// 如果数据库没有此航班，则不保存
		// 因为携程获取不到三字码信息
		utils.AppendToFile(logs.SaveInfoLogFile,
			fmt.Sprintf("[ctrip %s]:no data in RealTime table [%s:%s:%s:%s]\n",
				time.Now().Format("2006-01-02 15:04:05"),
				data.FlightDate, data.FlightNo, data.DepPlanTime, data.ArrPlanTime))
		return
	} else if err == nil {
		// 检查数据库中的航班是否为最新状态
		if CtripHasChange(data, dbFlightData) == false {
			// 状态没有更新，则不需要保存
			return
		}

		// 更新字段
		result, err := db.Exec(fmt.Sprintf("update [dbo].[RealTime]"+
			" set"+
			" flightState='%s',"+
			" depPlanTime='%s',"+
			" depExpTime='%s',"+
			" depActualTime='%s',"+
			" arrPlanTime='%s',"+
			" arrExpTime='%s',"+
			" arrActualTime='%s',"+
			" depWeather='%s',"+
			" arrWeather='%s',"+
			" checkinCounter='%s',"+
			" boardGate='%s',"+
			" baggageTurntable='%s',"+
			" source='%s',"+
			" updateAt='%s'"+
			" where flightNo='%s' and date='%s' and depPlanTime='%s' and arrPlanTime='%s'",
			data.FlightState,
			data.DepPlanTime,
			data.DepActualTime,
			data.DepActualTime,
			data.ArrPlanTime,
			data.ArrActualTime,
			data.ArrActualTime,
			data.DepWeather,
			data.ArrWeather,
			data.CheckinCounter,
			data.BoardGate,
			data.BaggageTurntable,
			"ctrip",
			time.Now().Format("2006-01-02 15:04:05"),
			data.FlightNo, data.FlightDate, data.DepPlanTime, data.ArrPlanTime))
		if err != nil {
			utils.AppendToFile(logs.SaveInfoLogFile,
				fmt.Sprintf("[ctrip %s]:update flight item error:%q [%s:%s:%s:%s]\n",
					time.Now().Format("2006-01-02 15:04:05"), err,
					data.FlightDate, data.FlightNo, data.DepPlanTime, data.ArrPlanTime))
			return
		}

		// 检查更新了几条记录
		n, err := result.RowsAffected()
		if err != nil {
			utils.AppendToFile(logs.SaveInfoLogFile,
				fmt.Sprintf("[ctrip %s]:check RowsAffected error:%q [%s:%s:%s:%s]\n",
					time.Now().Format("2006-01-02 15:04:05"), err,
					data.FlightDate, data.FlightNo, data.DepPlanTime, data.ArrPlanTime))
			return
		} else if n != 1 {
			utils.AppendToFile(logs.SaveInfoLogFile,
				fmt.Sprintf("[ctrip %s]:check RowsAffected fail:%d [%s:%s:%s:%s]\n",
					time.Now().Format("2006-01-02 15:04:05"), n,
					data.FlightDate, data.FlightNo, data.DepPlanTime, data.ArrPlanTime))
			return
		}

		// 更新抓取时间字段
		if data.FlightState == "起飞" && dbFlightData.FlightState != "起飞" {
			_, err = db.Exec(fmt.Sprintf("update [dbo].[RealTime]"+
				" set"+
				" depAt='%s'"+
				" where flightNo='%s' and date='%s' and depPlanTime='%s' and arrPlanTime='%s'",
				time.Now().Format("2006-01-02 15:04:05"),
				data.FlightNo, data.FlightDate, data.DepPlanTime, data.ArrPlanTime))
			if err != nil {
				utils.AppendToFile(logs.SaveInfoLogFile,
					fmt.Sprintf("[ctrip %s]:update depAt field error:%q [%s:%s:%s:%s]\n",
						time.Now().Format("2006-01-02 15:04:05"), err,
						data.FlightDate, data.FlightNo, data.DepPlanTime, data.ArrPlanTime))
			}
		} else if data.FlightState == "到达" && dbFlightData.FlightState != "到达" {
			_, err = db.Exec(fmt.Sprintf("update [dbo].[RealTime]"+
				" set"+
				" arrAt='%s'"+
				" where flightNo='%s' and date='%s' and depPlanTime='%s' and arrPlanTime='%s'",
				time.Now().Format("2006-01-02 15:04:05"),
				data.FlightNo, data.FlightDate, data.DepPlanTime, data.ArrPlanTime))
			if err != nil {
				utils.AppendToFile(logs.SaveInfoLogFile,
					fmt.Sprintf("[ctrip %s]:update arrAt field error:%q [%s:%s:%s:%s]\n",
						time.Now().Format("2006-01-02 15:04:05"), err,
						data.FlightDate, data.FlightNo, data.DepPlanTime, data.ArrPlanTime))
			}
		}
	} else {
		utils.AppendToFile(logs.SaveInfoLogFile,
			fmt.Sprintf("[ctrip %s]:db.QueryRow error:%q [%s:%s:%s:%s]\n",
				time.Now().Format("2006-01-02 15:04:05"), err,
				data.FlightDate, data.FlightNo, data.DepPlanTime, data.ArrPlanTime))
	}
}

// 保存从携程(ctrip)抓取的 ParseResult
func SaveParseResultFromCtrip(result types.ParseResult) {
	for _, item := range result.Items {
		data := item.(ctripParser.FlightDetailData)
		UpdateFlightItemFromCtrip(data)
	}
}

// 飞常准(veryzhun)的航班有更新
func VeryzhunHasChange(newData veryParser.FlightDetailData, oldData simpleFlightDetailData) bool {

	// 比较时间 2018-10-17 14:55:00.000
	if newData.FlightDeptimeDate != "1990-01-01 00:00" && newData.FlightDeptimeDate != oldData.DepActualTime[0:16] {
		return true
	}
	if newData.FlightArrtimeDate != "1990-01-01 00:00" && newData.FlightArrtimeDate != oldData.ArrActualTime[0:16] {
		return true
	}

	// 比较航班状态
	if newData.FlightState != oldData.FlightState {
		return true
	}

	// 比较值机柜台信息
	if oldData.CheckinCounter != newData.CheckinTable || oldData.BoardGate != newData.BoardGate ||
		oldData.BaggageTurntable != newData.BaggageID {
		return true
	}
	return false
}

// 保存从飞常准(veryzhun)抓取的 FlightDetailData
func UpdateFlightItemFromVeryzhun(data veryParser.FlightDetailData) {

	// #1. 修正时间字段
	//
	// 如果没有起飞，则实际起飞时间字段 FlightDeptimeDate 为空
	// 如果没有到达，则实际到达时间字段 FlightArrtimeDate 为空
	//
	// 飞常准抓取的时间字段格式为: 2018-10-24 18:15:00，故需要把秒去掉
	depPlanTime := data.FlightDeptimePlanDate
	depActualTime := data.FlightDeptimeDate
	arrPlanTime := data.FlightArrtimePlanDate
	arrActualTime := data.FlightArrtimeDate

	if depActualTime == "" {
		depActualTime = "1990-01-01 00:00:00"
	}
	if arrActualTime == "" {
		arrActualTime = "1990-01-01 00:00:00"
	}

	// 去掉时间信息中的秒
	depPlanTime = depPlanTime[0:16]
	depActualTime = depActualTime[0:16]
	arrPlanTime = arrPlanTime[0:16]
	arrActualTime = arrActualTime[0:16]

	// #2. 获取数据库中该航班的最新状态并进行比较
	var dbFlightData simpleFlightDetailData
	err := db.QueryRow(fmt.Sprintf(
		"select top 1 "+
			"flightNo,date,depCode,arrCode,flightState,"+
			"depPlanTime,depActualTime,arrPlanTime,arrActualTime,"+
			"checkinCounter,boardGate,baggageTurntable "+
			"from [dbo].[RealTime] "+
			"where flightNo='%s' and date='%s' and depCode='%s' and arrCode='%s' "+
			"order by updateAt desc",
		data.FlightNo, data.FlightDate, data.FlightDepcode, data.FlightArrcode)).Scan(
		&dbFlightData.FlightNo,
		&dbFlightData.Date,
		&dbFlightData.DepCode,
		&dbFlightData.ArrCode,
		&dbFlightData.FlightState,
		&dbFlightData.DepPlanTime,
		&dbFlightData.DepActualTime,
		&dbFlightData.ArrPlanTime,
		&dbFlightData.ArrActualTime,
		&dbFlightData.CheckinCounter,
		&dbFlightData.BoardGate,
		&dbFlightData.BaggageTurntable)

	if err == sql.ErrNoRows {
		// 数据库不存在: 只有当天及之后的航班，才进行插入操作
		// 因为之前的不存在的航班，可能是已经归档了
		if data.FlightDate >= time.Now().Format("2006-01-02") {
			// 插入新记录
			_, err = db.Exec(fmt.Sprintf("insert into [dbo].[RealTime]"+
				"(flightNo,date,depCode,arrCode,depCity,arrCity,flightState,"+
				"depPlanTime,depExpTime,depActualTime,arrPlanTime,arrExpTime,arrActualTime,"+
				"depWeather,arrWeather,"+
				"checkinCounter,boardGate,baggageTurntable,"+
				"updateAt,"+
				"source)"+
				" values ("+
				"'%s','%s','%s','%s','%s','%s','%s',"+
				"'%s','%s','%s','%s','%s','%s',"+
				"'%s','%s',"+
				"'%s','%s','%s',"+
				"'%s','%s')",
				data.FlightNo,
				data.FlightDate,
				data.FlightDepcode,
				data.FlightArrcode,
				data.FlightDep,
				data.FlightArr,
				data.FlightState,
				depPlanTime,
				depActualTime,
				depActualTime,
				arrPlanTime,
				arrActualTime,
				arrActualTime,
				data.DepWeather,
				data.ArrWeather,
				data.CheckinTable,
				data.BoardGate,
				data.BaggageID,
				time.Now().Format("2006-01-02 15:04:05"),
				"veryzhun"))
			if err != nil {
				utils.AppendToFile(logs.SaveInfoLogFile,
					fmt.Sprintf("[veryzhun %s]:insert into RealTime table error:%q [%s:%s:%s:%s]\n",
						time.Now().Format("2006-01-02 15:04:05"), err,
						data.FlightDate, data.FlightNo, data.FlightDepcode, data.FlightArrcode))
			}
		} else {
			// 从航联纵横抓取到的航班，被丢弃了
			utils.AppendToFile(logs.SaveInfoLogFile,
				fmt.Sprintf("[veryzhun %s]:skip early entry from veryzhun [%s:%s:%s:%s]\n",
					time.Now().Format("2006-01-02 15:04:05"),
					data.FlightDate, data.FlightNo, data.FlightDepcode, data.FlightArrcode))
		}
	} else if err == nil {
		if VeryzhunHasChange(data, dbFlightData) == false {
			// 状态没有更新，则不需要保存
			return
		}

		// 更新字段
		result, err := db.Exec(fmt.Sprintf("update [dbo].[RealTime]"+
			" set"+
			" depCity='%s',"+
			" arrCity='%s',"+
			" flightState='%s',"+
			" depPlanTime='%s',"+
			" arrPlanTime='%s',"+
			" depWeather='%s',"+
			" arrWeather='%s',"+
			" checkinCounter='%s',"+
			" boardGate='%s',"+
			" baggageTurntable='%s',"+
			" source='%s',"+
			" updateAt='%s'"+
			" where flightNo='%s' and date='%s' and depCode='%s' and arrCode='%s'",
			data.FlightDep,
			data.FlightArr,
			data.FlightState,
			depPlanTime,
			arrPlanTime,
			data.DepWeather,
			data.ArrWeather,
			data.CheckinTable,
			data.BoardGate,
			data.BaggageID,
			"veryzhun",
			time.Now().Format("2006-01-02 15:04:05"),
			data.FlightNo, data.FlightDate, data.FlightDepcode, data.FlightArrcode))
		if err != nil {
			utils.AppendToFile(logs.SaveInfoLogFile,
				fmt.Sprintf("[veryzhun %s]:update flight item error:%q [%s:%s:%s:%s]\n",
					time.Now().Format("2006-01-02 15:04:05"), err,
					data.FlightDate, data.FlightNo, data.FlightDepcode, data.FlightArrcode))
			return
		}

		// 检查更新了几条记录
		n, err := result.RowsAffected()
		if err != nil {
			utils.AppendToFile(logs.SaveInfoLogFile,
				fmt.Sprintf("[veryzhun %s]:check RowsAffected error:%q [%s:%s:%s:%s]\n",
					time.Now().Format("2006-01-02 15:04:05"), err,
					data.FlightDate, data.FlightNo, data.FlightDepcode, data.FlightArrcode))
			return
		} else if n != 1 {
			utils.AppendToFile(logs.SaveInfoLogFile,
				fmt.Sprintf("[veryzhun %s]:check RowsAffected fail:%d [%s:%s:%s:%s]\n",
					time.Now().Format("2006-01-02 15:04:05"), n,
					data.FlightDate, data.FlightNo, data.FlightDepcode, data.FlightArrcode))
			return
		}

		// 单独更新实际起降时间字段
		if depActualTime != "1990-01-01 00:00" {
			_, err = db.Exec(fmt.Sprintf("update [dbo].[RealTime]"+
				" set"+
				" depExpTime='%s',"+
				" depActualTime='%s'"+
				" where flightNo='%s' and date='%s' and depCode='%s' and arrCode='%s'",
				depActualTime,
				depActualTime,
				data.FlightNo, data.FlightDate, data.FlightDepcode, data.FlightArrcode))
			if err != nil {
				utils.AppendToFile(logs.SaveInfoLogFile,
					fmt.Sprintf("[veryzhun %s]:update deptime field error:%q [%s:%s:%s:%s]\n",
						time.Now().Format("2006-01-02 15:04:05"), err,
						data.FlightDate, data.FlightNo, data.FlightDepcode, data.FlightArrcode))
			}
		}
		if arrActualTime != "1990-01-01 00:00" {
			_, err = db.Exec(fmt.Sprintf("update [dbo].[RealTime]"+
				" set"+
				" arrExpTime='%s',"+
				" arrActualTime='%s'"+
				" where flightNo='%s' and date='%s' and depCode='%s' and arrCode='%s'",
				arrActualTime,
				arrActualTime,
				data.FlightNo, data.FlightDate, data.FlightDepcode, data.FlightArrcode))
			if err != nil {
				utils.AppendToFile(logs.SaveInfoLogFile,
					fmt.Sprintf("[veryzhun %s]:update arrtime field error:%q [%s:%s:%s:%s]\n",
						time.Now().Format("2006-01-02 15:04:05"), err,
						data.FlightDate, data.FlightNo, data.FlightDepcode, data.FlightArrcode))
			}
		}

		// 更新抓取时间
		if data.FlightState == "起飞" && dbFlightData.FlightState != "起飞" {
			_, err = db.Exec(fmt.Sprintf("update [dbo].[RealTime]"+
				" set"+
				" depAt='%s'"+
				" where flightNo='%s' and date='%s' and depCode='%s' and arrCode='%s'",
				time.Now().Format("2006-01-02 15:04:05"),
				data.FlightNo, data.FlightDate, data.FlightDepcode, data.FlightArrcode))
			if err != nil {
				utils.AppendToFile(logs.SaveInfoLogFile,
					fmt.Sprintf("[veryzhun %s]:update depAt field error:%q [%s:%s:%s:%s]\n",
						time.Now().Format("2006-01-02 15:04:05"), err,
						data.FlightDate, data.FlightNo, data.FlightDepcode, data.FlightArrcode))
			}
		}
		if data.FlightState == "到达" && dbFlightData.FlightState != "到达" {
			_, err = db.Exec(fmt.Sprintf("update [dbo].[RealTime]"+
				" set"+
				" arrAt='%s'"+
				" where flightNo='%s' and date='%s' and depCode='%s' and arrCode='%s'",
				time.Now().Format("2006-01-02 15:04:05"),
				data.FlightNo, data.FlightDate, data.FlightDepcode, data.FlightArrcode))
			if err != nil {
				utils.AppendToFile(logs.SaveInfoLogFile,
					fmt.Sprintf("[veryzhun %s]:update arrAt field error:%q [%s:%s:%s:%s]\n",
						time.Now().Format("2006-01-02 15:04:05"), err,
						data.FlightDate, data.FlightNo, data.FlightDepcode, data.FlightArrcode))
			}
		}

	} else {
		utils.AppendToFile(logs.SaveInfoLogFile,
			fmt.Sprintf("[veryzhun %s]:db.QueryRow error:%q [%s:%s:%s:%s]\n",
				time.Now().Format("2006-01-02 15:04:05"), err,
				data.FlightDate, data.FlightNo, data.FlightDepcode, data.FlightArrcode))
	}
}

// 保存从飞常准(veryzhun)抓取的 ParseResult
func SaveParseResultFromVeryzhun(result types.ParseResult) {
	// #1. 从数据库中检索出该航班的所有航段数据
	data := result.Items[0].(veryParser.FlightDetailData)
	rows, err := db.Query(fmt.Sprintf("select id,flightNo,date,depCode,arrCode,flightState from [dbo].[RealTime] "+
		"where flightNo='%s' and date='%s'", data.FlightNo, data.FlightDate))
	if err != nil {
		utils.AppendToFile(logs.SaveInfoLogFile,
			fmt.Sprintf("[veryzhun %s]:db.Query error:%q [%s:%s:%s:%s]\n",
				time.Now().Format("2006-01-02 15:04:05"), err,
				data.FlightDate, data.FlightNo, data.FlightDepcode, data.FlightArrcode))
		return
	}
	defer rows.Close()

	// 需要一个临时类型，用于接收数据库查询的数据
	type FlightData struct {
		ID          int
		FlightNo    string
		FlightDate  string
		DepCode     string
		ArrCode     string
		FlightState string
	}

	// #2. 遍历所有的航段数据
	var dbdata FlightData
	for rows.Next() {
		err = rows.Scan(&dbdata.ID, &dbdata.FlightNo, &dbdata.FlightDate, &dbdata.DepCode, &dbdata.ArrCode, &dbdata.FlightState)
		if err != nil {
			utils.AppendToFile(logs.SaveInfoLogFile,
				fmt.Sprintf("[veryzhun %s]:rows.Scan error:%q\n", time.Now().Format("2006-01-02 15:04:05"), err))
			continue
		}

		// #3. 找出不在 ParseResult 中的航段，并将之状态修改为 "暂无"
		exist := false
		for _, item := range result.Items {
			data := item.(veryParser.FlightDetailData)

			// 数据库数据能够匹配到飞常准查询的数据，则更新
			if data.FlightNo == dbdata.FlightNo && data.FlightDate == dbdata.FlightDate &&
				data.FlightDepcode == dbdata.DepCode && data.FlightArrcode == dbdata.ArrCode {
				exist = true
				break
			}
		}

		// 更新航班状态为 "暂无"
		if !exist {
			_, err = db.Exec(fmt.Sprintf("update [dbo].[RealTime] set flightState='暂无' where id=%d", dbdata.ID))
			if err != nil {
				utils.AppendToFile(logs.SaveInfoLogFile,
					fmt.Sprintf("[veryzhun %s]:update RealTime table error:%q [%s:%s:%s:%s]\n",
						time.Now().Format("2006-01-02 15:04:05"), err,
						data.FlightDate, data.FlightNo, data.FlightDepcode, data.FlightArrcode))
			}
		}
	}

	// #4. 更新飞常准查询的数据
	//
	// 如果数据库有此数据，则更新；
	// 如果数据库没有此数据，则添加。
	for _, item := range result.Items {
		data := item.(veryParser.FlightDetailData)
		UpdateFlightItemFromVeryzhun(data)
	}

}
