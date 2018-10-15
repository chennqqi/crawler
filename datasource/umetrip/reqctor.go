package umetrip

import (
	"fmt"

	"github.com/champkeh/crawler/datasource/umetrip/parser"
	"github.com/champkeh/crawler/types"
)

// date format: "2018-09-09"
func ListRequest(airports chan types.Airport, date string) chan types.Request {

	// because this channel is used for scheduler's in-channel, which will be snatched
	// by 100 workers (goroutine), so set 100 buffer space is better.
	requests := make(chan types.Request, 1000)

	go func() {
		for airport := range airports {
			url := fmt.Sprintf("http://www.umetrip.com/mskyweb/fs/fa.do?dep=%s&arr=%s&date=%s",
				airport.DepCode, airport.ArrCode, date)

			requests <- types.Request{
				RawParam: types.Param{
					Dep:  airport.DepCode,
					Arr:  airport.ArrCode,
					Date: date,
				},
				Url:        url,
				ParserFunc: parser.ParseList,
			}
		}
	}()

	return requests
}

func DetailRequest(flights chan types.FlightInfo) chan types.Request {

	// because this channel is used for scheduler's in-channel, which will be snatched
	// by 100 workers (goroutine), so set 100 buffer space is better.
	requests := make(chan types.Request, 100)

	go func() {
		for flight := range flights {
			// 详情页url:http://www.umetrip.com/mskyweb/fs/fc.do?flightNo=MU3924&date=2018-09-13
			url := fmt.Sprintf("http://www.umetrip.com/mskyweb/fs/fc.do?flightNo=%s&date=%s",
				flight.FlightNo, flight.FlightDate)

			requests <- types.Request{
				Url:        url,
				ParserFunc: parser.ParseDetail,
				RawParam: types.Param{
					Date: flight.FlightDate,
					Fno:  flight.FlightNo,
				},
			}
		}
	}()

	return requests
}
