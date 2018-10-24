package ctrip

import (
	"fmt"

	"strings"

	"github.com/champkeh/crawler/source/ctrip/parser"
	"github.com/champkeh/crawler/types"
	"golang.org/x/text/encoding/simplifiedchinese"
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

	// 详情页url:http://flights.ctrip.com/actualtime/fno--MU5696-20181016.html
	url := fmt.Sprintf("http://flights.ctrip.com/actualtime/fno--%s-%s.html",
		flight.FlightNo, strings.Replace(flight.FlightDate, "-", "", -1))

	request := types.Request{
		RawParam: types.Param{
			Date: flight.FlightDate,
			Fno:  flight.FlightNo,
		},
		Url:        url,
		Referer:    "http://flights.ctrip.com/",
		ParserFunc: parser.ParseDetail,
		Source:     "ctrip",
		Encoding:   simplifiedchinese.GBK,
	}

	return request
}
