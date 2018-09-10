package parser

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/champkeh/crawler/engine"
)

const detailRe = `(?sU)<div class="reg">(.*)</div>.*alt="航空公司" />.*<b>(.*)</b>.*<h1.*>(.*)</h1>`

type FlightDetailData struct {
}

func ParseDetail(contents []byte) engine.ParseResult {
	re := regexp.MustCompile(detailRe)
	sm := re.FindAllStringSubmatch(string(contents), 2)

	result := engine.ParseResult{}
	for _, m := range sm {
		fmt.Println(strings.TrimSpace(m[1]), strings.TrimSpace(m[2]), strings.TrimSpace(m[3]))
		result.Items = append(result.Items, FlightDetailData{})
	}
	return result
}
