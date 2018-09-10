package parser

import (
	"fmt"
	"strings"

	"regexp"

	"github.com/champkeh/crawler/types"
)

const detailRe = `(?sU)<div class="reg">(.*)</div>.*alt="航空公司" />.*<b>(.*)</b>.*<h1.*>(.*)</h1>`

type FlightDetailData struct {
}

// place this compile step outside the function to speed reason
var detailReCompile = regexp.MustCompile(detailRe)

func ParseDetail(contents []byte) types.ParseResult {
	sm := detailReCompile.FindAllStringSubmatch(string(contents), 2)

	result := types.ParseResult{}
	for _, m := range sm {
		fmt.Println(strings.TrimSpace(m[1]), strings.TrimSpace(m[2]), strings.TrimSpace(m[3]))
		result.Items = append(result.Items, FlightDetailData{})
	}
	return result
}
