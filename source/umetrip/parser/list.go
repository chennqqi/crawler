package parser

import (
	"regexp"

	"github.com/champkeh/crawler/types"
)

//http://www.umetrip.com/mskyweb/fs/fa.do?dep=SHA&arr=PEK&date=2018-09-09

const listRe = `(?sU)temp\.push.*title='(.*)'.*title='(.*)'.*<span class='w125'> (.*)</span><span class='w125'> (.*)</span>.*
.*"(.*)</span>.*
.*"(.*)</span> <span class='w125'>(.*)</span>.*
.*"(.*)</span>`

// place this compile step outside the function to speed reason
var listReCompile = regexp.MustCompile(listRe)

type FlightListData struct {
	FlightNo      string
	FlightCompany string
	DepTimePlan   string
	DepTimeActual string
	ArrTimePlan   string
	ArrTimeActual string
	State         string
	Airport       string
}

// ParseList parse flight list data
func ParseList(contents []byte) (types.ParseResult, error) {
	matches := listReCompile.FindAllSubmatch(contents, -1)

	result := types.ParseResult{}
	for _, m := range matches {
		result.Items = append(result.Items, FlightListData{
			FlightNo:      string(m[1]),
			FlightCompany: string(m[2]),
			DepTimePlan:   string(m[3]),
			DepTimeActual: string(m[4]),
			Airport:       string(m[5]),
			ArrTimePlan:   string(m[6]),
			ArrTimeActual: string(m[7]),
			State:         string(m[8]),
		})
	}
	return result, nil
}
