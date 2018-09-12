package parser

import (
	"regexp"

	"bytes"

	"github.com/PuerkitoBio/goquery"
	"github.com/champkeh/crawler/types"
)

const detailRe = `(?sU)<div class="reg">(.*)</div>.*alt="航空公司" />.*<b>(.*)</b>.*<h1.*>(.*)</h1>`

type FlightDetailData struct {
	Mileage string
}

// place this compile step outside the function to speed reason
var detailReCompile = regexp.MustCompile(detailRe)

func ParseDetail(contents []byte) types.ParseResult {
	reader := bytes.NewReader(contents)
	doc, err := goquery.NewDocumentFromReader(reader)
	if err != nil {
		panic(err)
	}
	result := types.ParseResult{}

	detail := FlightDetailData{}
	detail.Mileage = doc.Find(".p_info .mileage").First().Text()

	result.Items = append(result.Items, detail)

	return result
}
