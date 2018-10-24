package main

import (
	"fmt"
	"regexp"

	"bytes"

	"github.com/PuerkitoBio/goquery"
	"github.com/champkeh/crawler/fetcher"
	"github.com/champkeh/crawler/persist"
	"github.com/champkeh/crawler/source/veryzhun"
	"github.com/champkeh/crawler/types"
)

func main() {
	flight := types.FlightInfo{
		FlightNo:   "MF858",
		FlightDate: "2018-10-24",
	}
	request := veryzhun.DetailRequest(flight)
	result, err := fetcher.FetchRequest(request, nil)
	if err != nil {
		panic(err)
	}

	persist.SaveToRealTime(result)
}

func foo() {
	flight := types.FlightInfo{
		FlightNo:   "GS1113",
		FlightDate: "2018-10-22",
	}
	request := veryzhun.DetailRequest(flight)
	result, err := fetcher.FetchRequest(request, nil)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Printf("%v", result)
	}
}

type FlightDetail struct {
	FlightNo      string
	FlightCompany string
	DeptimePlan   string
	DeptimeActual string
	ArrtimePlan   string
	ArrtimeActual string
	FlightState   string
	DepName       string
	ArrName       string
}

func parse(contents []byte) ([]FlightDetail, error) {
	reader := bytes.NewReader(contents)
	doc, err := goquery.NewDocumentFromReader(reader)
	if err != nil {
		panic(err)
	}

	var result = make([]FlightDetail, 0)
	doclist := doc.Find("#list li")
	for i := 0; i < doclist.Length(); i++ {
		li := doclist.Eq(i)
		flight := FlightDetail{
			FlightCompany: li.Find(".li_com span").Eq(0).Find("b a").Eq(0).Text(),
			FlightNo:      li.Find(".li_com span").Eq(0).Find("b a").Eq(1).Text(),
			DeptimePlan:   trimSpace(li.Find(".li_com span").Eq(1).Text()),
			DeptimeActual: li.Find(".li_com span").Eq(2).Find("img").AttrOr("src", ""),
			DepName:       li.Find(".li_com span").Eq(3).Text(),
			ArrtimePlan:   trimSpace(li.Find(".li_com span").Eq(4).Text()),
			ArrtimeActual: li.Find(".li_com span").Eq(5).Find("img").AttrOr("src", ""),
			ArrName:       li.Find(".li_com span").Eq(6).Text(),
			FlightState:   li.Find(".li_com span").Eq(8).Text(),
		}

		result = append(result, flight)
	}

	return result, nil
}
func trimSpace(text string) string {
	if text == "" {
		return ""
	}

	reg := regexp.MustCompile(`[\n\t ]`)
	return reg.ReplaceAllString(text, "")
}
