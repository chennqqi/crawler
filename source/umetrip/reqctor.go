package umetrip

import (
	"fmt"

	"github.com/champkeh/crawler/source/umetrip/parser"
	"github.com/champkeh/crawler/types"
	"golang.org/x/text/encoding/unicode"
)

// date format: "2018-09-09"
func ListRequestPipe(airports chan types.Airport, date string) chan types.Request {

	requests := make(chan types.Request, 1000)

	go func() {
		for airport := range airports {
			requests <- ListRequest(airport, date)
		}
	}()

	return requests
}

func DetailRequestPipe(flights chan types.FlightInfo) chan types.Request {

	requests := make(chan types.Request, 1000)

	go func() {
		for flight := range flights {
			requests <- DetailRequest(flight)
		}
	}()

	return requests
}

func ListRequest(airport types.Airport, date string) types.Request {
	url := fmt.Sprintf("http://www.umetrip.com/mskyweb/fs/fa.do?dep=%s&arr=%s&date=%s",
		airport.DepCode, airport.ArrCode, date)

	request := types.Request{
		RawParam: types.Param{
			Date: date,
			Dep:  airport.DepCode,
			Arr:  airport.ArrCode,
		},
		Url:        url,
		Referer:    "http://www.umetrip.com/",
		ParserFunc: parser.ParseList,
		Source:     "umetrip",
		Encoding:   unicode.UTF8,
	}

	return request
}

func DetailRequest(flight types.FlightInfo) types.Request {
	// 详情页url:http://www.umetrip.com/mskyweb/fs/fc.do?flightNo=MU3924&date=2018-09-13
	url := fmt.Sprintf("http://www.umetrip.com/mskyweb/fs/fc.do?flightNo=%s&date=%s",
		flight.FlightNo, flight.FlightDate)

	request := types.Request{
		RawParam: types.Param{
			Date: flight.FlightDate,
			Fno:  flight.FlightNo,
		},
		Url:        url,
		Referer:    "http://www.umetrip.com/",
		ParserFunc: parser.ParseDetail,
		Source:     "umetrip",
		Encoding:   unicode.UTF8,
	}

	return request
}
