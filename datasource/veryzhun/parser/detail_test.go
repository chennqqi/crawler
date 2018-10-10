package parser

import (
	"io/ioutil"
	"testing"
	"time"
)

func MustParse(layout string, value string) time.Time {
	t, err := time.Parse(layout, value)
	if err != nil {
		panic(err)
	}
	return t
}

func TestParseDetail(t *testing.T) {
	// test single data
	singleData, err := ioutil.ReadFile("./testdata/single.json")
	if err != nil {
		t.Errorf("test data not exist: %v", err)
	}

	result, err := ParseDetail(singleData)
	if err != nil {
		t.Errorf("parse error: %v", err)
	}

	singleExpected := FlightDetailData{
		ArrWeather:       "晴|||47|18/5",
		DepWeather:       "晴|||143|5/-6",
		ArrCity:          "昆明",
		ArrAirport:       "昆明长水",
		ArrCode:          "KMG",
		ArrtimeDate:      JsonTime(MustParse(layout, "2018-01-18 10:21:00")),
		ArrtimePlanDate:  JsonTime(MustParse(layout, "2018-01-18 10:20:00")),
		ArrtimeReadyDate: JsonTime(MustParse(layout, "2018-01-18 10:19:00")),
		DepCity:          "北京",
		DepAirport:       "北京首都",
		DepCode:          "PEK",
		DeptimeDate:      JsonTime(MustParse(layout, "2018-01-18 06:43:00")),
		DeptimePlanDate:  JsonTime(MustParse(layout, "2018-01-18 06:25:00")),
		DeptimeReadyDate: JsonTime(MustParse(layout, "2018-01-18 06:25:00")),
		FlightCompany:    "中国国航",
		FlightNo:         "CA1403",
		FlightState:      "到达",
	}
	if len(result.Items) != 1 {
		t.Errorf("got %d item; expected %d", len(result.Items), 1)
	}
	actual, ok := result.Items[0].(FlightDetailData)
	if !ok {
		t.Errorf("type not match: %v", ok)
	}

	if actual.ArrWeather != singleExpected.ArrWeather {
		t.Errorf("got %v; expected %v", actual.ArrWeather, singleExpected.ArrWeather)
	}
	if actual.DepWeather != singleExpected.DepWeather {
		t.Errorf("got %v; expected %v", actual.DepWeather, singleExpected.DepWeather)
	}
	if actual.ArrCity != singleExpected.ArrCity {
		t.Errorf("got %v; expected %v", actual.ArrCity, singleExpected.ArrCity)
	}
	if actual.ArrAirport != singleExpected.ArrAirport {
		t.Errorf("got %v; expected %v", actual.ArrAirport, singleExpected.ArrAirport)
	}
	if actual.ArrCode != singleExpected.ArrCode {
		t.Errorf("got %v; expected %v", actual.ArrCode, singleExpected.ArrCode)
	}
	if actual.DepCity != singleExpected.DepCity {
		t.Errorf("got %v; expected %v", actual.DepCity, singleExpected.DepCity)
	}
	if actual.DepAirport != singleExpected.DepAirport {
		t.Errorf("got %v; expected %v", actual.DepAirport, singleExpected.DepAirport)
	}
	if actual.DepCode != singleExpected.DepCode {
		t.Errorf("got %v; expected %v", actual.DepCode, singleExpected.DepCode)
	}
	if actual.FlightCompany != singleExpected.FlightCompany {
		t.Errorf("got %v; expected %v", actual.FlightCompany, singleExpected.FlightCompany)
	}
	if actual.FlightNo != singleExpected.FlightNo {
		t.Errorf("got %v; expected %v", actual.FlightNo, singleExpected.FlightNo)
	}
	if actual.FlightState != singleExpected.FlightState {
		t.Errorf("got %v; expected %v", actual.FlightState, singleExpected.FlightState)
	}
	if actual.DeptimeDate != singleExpected.DeptimeDate {
		t.Errorf("got %v; expected %v", actual.DeptimeDate, singleExpected.DeptimeDate)
	}
	if actual.DeptimePlanDate != singleExpected.DeptimePlanDate {
		t.Errorf("got %v; expected %v", actual.DeptimePlanDate, singleExpected.DeptimePlanDate)
	}
	if actual.DeptimeReadyDate != singleExpected.DeptimeReadyDate {
		t.Errorf("got %v; expected %v", actual.DeptimeReadyDate, singleExpected.DeptimeReadyDate)
	}
	// test multiple data

	// test limit error
}
