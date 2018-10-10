package parser

import (
	"encoding/json"
	"errors"

	"fmt"

	"time"

	"github.com/champkeh/crawler/types"
)

// veryzhun's flight data can use api directly.
// so here use json package instead of regexp package.

const (
	layout = "2006-01-02 15:04:05"
)

type JsonTime time.Time

func (t JsonTime) MarshalJSON() ([]byte, error) {
	var stamp = fmt.Sprintf("\"%s\"", time.Time(t).Format(layout))
	return []byte(stamp), nil
}

func (t *JsonTime) UnmarshalJSON(data []byte) error {
	var (
		year int
		mon  int
		mday int
		hour int
		min  int
		sec  int
	)
	if len(data) <= 2 || data[0] != '"' || data[len(data)-1] != '"' {
		//return fmt.Errorf("invalid time: %s", data)
		return nil
	}
	var str = string(data[1 : len(data)-1])
	if n, err := fmt.Sscanf(str, "%d-%02d-%02d %02d:%02d:%02d", &year, &mon, &mday, &hour, &min, &sec); err != nil {
		//return fmt.Errorf("invalid string(%s): %s", err.Error(), data)
		return nil
	} else if n != 6 {
		//return fmt.Errorf("invalid time: %s", data)
		return nil
	}
	*t = JsonTime(time.Date(year, time.Month(mon), mday, hour, min, sec, 0, time.Local))
	return nil
}

func (t JsonTime) String() string {
	return time.Time(t).Format(layout)
}

type FlightDetailData struct {
	ArrWeather string `json:"ArrWeather"`
	DepWeather string `json:"DepWeather"`

	ArrCity          string   `json:"FlightArr"`
	ArrAirport       string   `json:"FlightArrAirport"`
	ArrCode          string   `json:"FlightArrcode"`
	ArrtimeDate      JsonTime `json:"FlightArrtimeDate"`
	ArrtimePlanDate  JsonTime `json:"FlightArrtimePlanDate"`
	ArrtimeReadyDate JsonTime `json:"FlightArrtimeReadyDate"`

	DepCity          string   `json:"FlightDep"`
	DepAirport       string   `json:"FlightDepAirport"`
	DepCode          string   `json:"FlightDepCode"`
	DeptimeDate      JsonTime `json:"FlightDeptimeDate"`
	DeptimePlanDate  JsonTime `json:"FlightDeptimePlanDate"`
	DeptimeReadyDate JsonTime `json:"FlightDeptimeReadyDate"`

	FlightCompany string `json:"FlightCompany"`
	FlightNo      string `json:"FlightNo"`
	//FlightYear    float32 `json:"FlightYear,omitempty"`
	FlightState string `json:"FlightState"`
}

type LimitError struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

func ParseDetail(contents []byte) (types.ParseResult, error) {
	result := types.ParseResult{}

	var data []FlightDetailData
	err := json.Unmarshal(contents, &data)
	if err != nil {
		var limitError LimitError
		err = json.Unmarshal(contents, &limitError)
		if err != nil {
			return result, err
		}
		return result, errors.New(fmt.Sprintf("[veryzhun]parser error: api limit: %v", limitError.Msg))
	}

	for _, item := range data {
		result.Items = append(result.Items, item)
	}

	return result, nil
}
