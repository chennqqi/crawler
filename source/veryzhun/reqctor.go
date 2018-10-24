package veryzhun

import (
	"fmt"

	"github.com/champkeh/crawler/source/veryzhun/parser"
	"github.com/champkeh/crawler/types"
	"golang.org/x/text/encoding/unicode"
)

func DetailRequestPipe(flights chan types.FlightInfo) chan types.Request {

	requests := make(chan types.Request, 1000)

	go func() {
		for flight := range flights {
			requests <- DetailRequest(flight)
		}
	}()

	return requests
}

func DetailRequest(flight types.FlightInfo) types.Request {

	//url := "http://webapp.veryzhun.com/h5/flightsearch?fnum=EU2264&date=2018-10-18&token=5cf2036c3db9fe08a7ee0c9b2077d37d"
	url := fmt.Sprintf("http://webapp.veryzhun.com/h5/flightsearch?"+
		"fnum=%s&date=%s&token=5cf2036c3db9fe08a7ee0c9b2077d37d", flight.FlightNo, flight.FlightDate)

	request := types.Request{
		RawParam: types.Param{
			Date: flight.FlightDate,
			Fno:  flight.FlightNo,
		},
		Url:        url,
		Referer:    "http://webapp.veryzhun.com",
		ParserFunc: parser.ParseDetail,
		Source:     "veryzhun",
		Encoding:   unicode.UTF8,
	}

	return request
}
