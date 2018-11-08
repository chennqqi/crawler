package parser

import (
	"encoding/json"
	"errors"

	"time"

	"fmt"

	"github.com/champkeh/crawler/types"
)

// veryzhun flight data can use api directly.
// so here use json package instead of regexp package.

type FlightDetailData struct {
	BaggageID              string //行李转盘
	BoardGate              string //登机口
	BoardState             string //登机状态
	CheckinTable           string //值机柜台
	Bridge                 string `json:"bridge"` //等级方式：靠廊桥
	DepWeather             string //起飞地天气
	ArrWeather             string //将落地天气
	FlightDep              string //起飞城市
	FlightDepAirport       string //起飞机场
	FlightDepcode          string //起飞机场三字码
	FlightDeptimeDate      string //实际起飞时间
	FlightDeptimePlanDate  string //计划起飞时间
	FlightDeptimeReadyDate string
	FlightArr              string //降落城市
	FlightArrAirport       string //降落机场
	FlightArrcode          string //降落机场三字码
	FlightArrtimeDate      string //实际到达时间
	FlightArrtimePlanDate  string //计划到达时间
	FlightArrtimeReadyDate string
	FlightCompany          string //航空公司
	FlightNo               string //航班号
	FlightDate             string //航班日期
	FlightState            string //航班状态
	FlightHTerminal        string //航站楼
	FlightTerminal         string //航站楼
	Generic                string `json:"generic"`
}

type LimitError struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

type NoDataError struct {
	Code int    `json:"error_code"`
	Msg  string `json:"error"`
}

var ErrNoData = errors.New("veryzhun: no data")

func ParseDetail(contents []byte) (types.ParseResult, error) {
	result := types.ParseResult{}

	// 按照正常数据解析
	var details []FlightDetailData
	err := json.Unmarshal(contents, &details)

	// 正常数据解析失败
	if err != nil {

		// 按照暂无数据进行解析
		var nodata NoDataError
		err = json.Unmarshal(contents, &nodata)
		if err != nil {
			return result, errors.New(fmt.Sprintf("[veryzhun]parse error: (%s:%s)", err, contents))
		} else if nodata.Code == 10 {
			return result, ErrNoData
		}

		// 按照api限制进行解析
		var limitError LimitError
		err = json.Unmarshal(contents, &limitError)
		if err != nil {
			return result, errors.New(fmt.Sprintf("[veryzhun]parse error: (%s:%s)", err, contents))
		}
		return result, errors.New("[veryzhun]parse error: api limit")
	}

	for _, item := range details {
		fdate, err := time.Parse("2006-01-02 15:04:05", item.FlightDeptimePlanDate)
		if err != nil {
			continue
		}
		item.FlightDate = fdate.Format("2006-01-02")
		result.Items = append(result.Items, item)
	}

	return result, nil
}
