package parser

import (
	"bytes"

	"strings"

	"errors"

	"fmt"

	"regexp"

	"github.com/PuerkitoBio/goquery"
	"github.com/champkeh/crawler/types"
)

type FlightDetailData struct {
	FlightNo         string
	FlightDate       string
	DepCode          string
	ArrCode          string
	DepCity          string
	ArrCity          string
	FlightState      string
	DepPlanTime      string
	ArrPlanTime      string
	DepActualTime    string
	ArrActualTime    string
	Mileage          string
	Duration         string
	Age              string
	DepWeather       string
	ArrWeather       string
	DepFlow          string
	ArrFlow          string
	PreFlightNo      string
	PreFlightDepCode string
	PreFlightArrCode string
	PreFlightState   string
}

func ParseDetail(contents []byte) types.ParseResult {
	reader := bytes.NewReader(contents)
	doc, err := goquery.NewDocumentFromReader(reader)
	if err != nil {
		panic(err)
	}
	result := types.ParseResult{}

	flyBoxes := doc.Find(".fly_box")
	if flyBoxes.Length() == 2 {
		// 只有一段
		detail := parseSingleFlight(doc)
		result.Items = append(result.Items, detail)
	} else if flyBoxes.Length() == 3 {
		// 3段航班
		details := parseMultiFlight(doc)
		for _, d := range details {
			result.Items = append(result.Items, d)
		}
	} else {
		panic(errors.New(fmt.Sprintf("该航班解析失败")))
	}

	return result
}

func parseSingleFlight(doc *goquery.Document) FlightDetailData {
	detail := FlightDetailData{}

	tit := doc.Find(".tit")
	detail.FlightNo = strings.TrimSpace(tit.Find("span b").Text())
	detail.FlightState = strings.TrimSpace(tit.Find("div.reg").Text())

	p_info := doc.Find(".p_info")
	detail.Mileage = strings.TrimSpace(p_info.Find(".mileage span").Text())
	detail.Duration = strings.TrimSpace(p_info.Find(".time span").Text())
	detail.Age = strings.TrimSpace(p_info.Find(".age span").Text())

	flyBoxes := doc.Find(".fly_box")

	first := flyBoxes.Eq(0)
	detail.DepCity = ParseCityCode(strings.TrimSpace(first.Find(".f_tit h2").Text()))[0]
	detail.DepCode = ParseCityCode(strings.TrimSpace(first.Find(".f_tit h2").Text()))[1]
	depwe := first.Find(".f_com .f_r p")
	detail.DepWeather = trimSpace(depwe.Eq(0).Text())
	detail.DepFlow = trimSpace(depwe.Eq(2).Text())
	deptimes := first.Find(".f_com .f_m .time dl")
	detail.DepPlanTime = ParseTime(deptimes.Eq(0).Find("img").AttrOr("src", ""))
	detail.DepActualTime = ParseTime(deptimes.Eq(1).Find("img").AttrOr("src", ""))

	// 前序航班
	if first.Find(".f_tit div").Is("div") {
		text := first.Find(".f_tit div").Text()
		info := ParsePreFlightInfo(text)

		detail.PreFlightNo = info[0]
		detail.PreFlightDepCode = info[1]
		detail.PreFlightArrCode = info[2]
		detail.PreFlightState = info[3]
	}

	second := flyBoxes.Eq(1)
	detail.ArrCity = ParseCityCode(strings.TrimSpace(second.Find(".f_tit h2").Text()))[0]
	detail.ArrCode = ParseCityCode(strings.TrimSpace(second.Find(".f_tit h2").Text()))[1]
	arrwe := second.Find(".f_com .f_r p")
	detail.ArrWeather = trimSpace(arrwe.Eq(0).Text())
	detail.ArrFlow = trimSpace(arrwe.Eq(2).Text())
	arrtimes := second.Find(".f_com .f_m .time dl")
	detail.ArrPlanTime = ParseTime(arrtimes.Eq(0).Find("img").AttrOr("src", ""))
	detail.ArrActualTime = ParseTime(arrtimes.Eq(1).Find("img").AttrOr("src", ""))

	detail.FlightDate = ParseDate(first.Find(".f_tit span").Text())

	return detail
}

func parseMultiFlight(doc *goquery.Document) [3]FlightDetailData {
	details := [3]FlightDetailData{}

	tit := doc.Find(".tit")
	details[0].FlightNo = strings.TrimSpace(tit.Find("span b").Text())
	details[1].FlightNo = strings.TrimSpace(tit.Find("span b").Text())
	details[2].FlightNo = strings.TrimSpace(tit.Find("span b").Text())

	details[0].FlightState = strings.TrimSpace(tit.Find("div.reg").Eq(0).Text())
	details[1].FlightState = strings.TrimSpace(tit.Find("div.reg").Eq(1).Text())
	// todo:第三段航班的状态需要判断
	if details[0].FlightState != "到达" {
		details[2].FlightState = details[0].FlightState
	} else {
		details[2].FlightState = details[1].FlightState
	}

	p_info := doc.Find(".p_info")
	details[0].Mileage = strings.TrimSpace(p_info.Find("ul").Eq(0).Find(".mileage span").Text())
	details[0].Duration = strings.TrimSpace(p_info.Find("ul").Eq(0).Find(".time span").Text())
	details[0].Age = strings.TrimSpace(p_info.Find("ul").Eq(0).Find(".age span").Text())
	details[1].Mileage = strings.TrimSpace(p_info.Find("ul").Eq(1).Find(".mileage span").Text())
	details[1].Duration = strings.TrimSpace(p_info.Find("ul").Eq(1).Find(".time span").Text())
	details[1].Age = strings.TrimSpace(p_info.Find("ul").Eq(1).Find(".age span").Text())
	details[2].Mileage = strings.TrimSpace(p_info.Find("ul").Eq(0).Find(".mileage span").Text())
	details[2].Duration = strings.TrimSpace(p_info.Find("ul").Eq(0).Find(".time span").Text())
	details[2].Age = strings.TrimSpace(p_info.Find("ul").Eq(0).Find(".age span").Text())

	flyBoxes := doc.Find(".fly_box")

	// first segment
	first := flyBoxes.Eq(0)
	details[0].DepCity = ParseCityCode(strings.TrimSpace(first.Find(".f_tit h2").Text()))[0]
	details[0].DepCode = ParseCityCode(strings.TrimSpace(first.Find(".f_tit h2").Text()))[1]
	details[2].DepCity = ParseCityCode(strings.TrimSpace(first.Find(".f_tit h2").Text()))[0]
	details[2].DepCode = ParseCityCode(strings.TrimSpace(first.Find(".f_tit h2").Text()))[1]
	depwe := first.Find(".f_com .f_r p")
	details[0].DepWeather = trimSpace(depwe.Eq(0).Text())
	details[2].DepWeather = trimSpace(depwe.Eq(0).Text())
	details[0].DepFlow = trimSpace(depwe.Eq(2).Text())
	details[2].DepFlow = trimSpace(depwe.Eq(2).Text())
	deptimes := first.Find(".f_com .f_m .time dl")
	details[0].DepPlanTime = ParseTime(deptimes.Eq(0).Find("img").AttrOr("src", ""))
	details[0].DepActualTime = ParseTime(deptimes.Eq(1).Find("img").AttrOr("src", ""))
	details[2].DepPlanTime = ParseTime(deptimes.Eq(0).Find("img").AttrOr("src", ""))
	details[2].DepActualTime = ParseTime(deptimes.Eq(1).Find("img").AttrOr("src", ""))

	details[0].FlightDate = ParseDate(first.Find(".f_tit span").Text())
	details[2].FlightDate = ParseDate(first.Find(".f_tit span").Text())

	// 前序航班
	if first.Find(".f_tit div").Is("div") {
		text := first.Find(".f_tit div").Text()
		info := ParsePreFlightInfo(text)

		details[0].PreFlightNo = info[0]
		details[0].PreFlightDepCode = info[1]
		details[0].PreFlightArrCode = info[2]
		details[0].PreFlightState = info[3]
	}

	// second segment
	second := flyBoxes.Eq(1)
	details[0].ArrCity = ParseCityCode(strings.TrimSpace(second.Find(".f_tit h2").Text()))[0]
	details[0].ArrCode = ParseCityCode(strings.TrimSpace(second.Find(".f_tit h2").Text()))[1]
	details[1].DepCity = ParseCityCode(strings.TrimSpace(second.Find(".f_tit h2").Text()))[0]
	details[1].DepCode = ParseCityCode(strings.TrimSpace(second.Find(".f_tit h2").Text()))[1]
	arrwe := second.Find(".f_com .f_r p")
	details[0].ArrWeather = trimSpace(arrwe.Eq(0).Text())
	details[1].DepWeather = trimSpace(arrwe.Eq(0).Text())
	details[0].ArrFlow = trimSpace(arrwe.Eq(2).Text())
	details[1].DepFlow = trimSpace(arrwe.Eq(2).Text())
	deparrtimes := second.Find(".f_com .f_m .time dl")
	details[0].ArrPlanTime = ParseTime(deparrtimes.Eq(0).Find("img").AttrOr("src", ""))
	details[0].ArrActualTime = ParseTime(deparrtimes.Eq(1).Find("img").AttrOr("src", ""))
	details[1].DepPlanTime = ParseTime(deparrtimes.Eq(2).Find("img").AttrOr("src", ""))
	details[1].DepActualTime = ParseTime(deparrtimes.Eq(3).Find("img").AttrOr("src", ""))

	details[1].FlightDate = ParseDate(second.Find(".f_tit span").Text())

	// 前序航班
	if second.Find(".f_tit div").Is("div") {
		text := second.Find(".f_tit div").Text()
		info := ParsePreFlightInfo(text)

		details[1].PreFlightNo = info[0]
		details[1].PreFlightDepCode = info[1]
		details[1].PreFlightArrCode = info[2]
		details[1].PreFlightState = info[3]
	}

	// third segment
	third := flyBoxes.Eq(2)
	details[1].ArrCity = ParseCityCode(strings.TrimSpace(third.Find(".f_tit h2").Text()))[0]
	details[1].ArrCode = ParseCityCode(strings.TrimSpace(third.Find(".f_tit h2").Text()))[1]
	details[2].ArrCity = ParseCityCode(strings.TrimSpace(third.Find(".f_tit h2").Text()))[0]
	details[2].ArrCode = ParseCityCode(strings.TrimSpace(third.Find(".f_tit h2").Text()))[1]
	arrwe2 := third.Find(".f_com .f_r p")
	details[1].ArrWeather = trimSpace(arrwe2.Eq(0).Text())
	details[2].ArrWeather = trimSpace(arrwe2.Eq(0).Text())
	details[1].ArrFlow = trimSpace(arrwe2.Eq(2).Text())
	details[2].ArrFlow = trimSpace(arrwe2.Eq(2).Text())
	arrtimes := third.Find(".f_com .f_m .time dl")
	details[1].ArrPlanTime = ParseTime(arrtimes.First().Find("img").AttrOr("src", ""))
	details[1].ArrActualTime = ParseTime(arrtimes.Last().Find("img").AttrOr("src", ""))
	details[2].ArrPlanTime = ParseTime(arrtimes.First().Find("img").AttrOr("src", ""))
	details[2].ArrActualTime = ParseTime(arrtimes.Last().Find("img").AttrOr("src", ""))

	return details
}

func ParseTime(mask string) string {
	if mask == "" {
		return ""
	}

	var re = `graphic\.do\?str=([^&]+)&`
	match := regexp.MustCompile(re).FindStringSubmatch(mask)

	return match[1]
}

func ParseCityCode(raw string) []string {
	var re = `(?sU)(.+)\((.+)\)`
	match := regexp.MustCompile(re).FindStringSubmatch(raw)

	result := []string{}
	for _, str := range match[1:] {
		result = append(result, trimSpace(str))
	}
	return result
}

func ParseDate(raw string) string {
	var re = `(\d{4}-\d{2}-\d{2})`
	match := regexp.MustCompile(re).FindStringSubmatch(raw)

	return strings.TrimSpace(match[1])
}

func ParsePreFlightInfo(text string) []string {
	var re = `前序航班([^[]+)\[([A-Z]+)到([A-Z]+)\]，(.+)`
	match := regexp.MustCompile(re).FindStringSubmatch(text)

	return match[1:]
}

func trimSpace(text string) string {
	if text == "" {
		return ""
	}

	reg := regexp.MustCompile(`[\n\t ]`)
	return reg.ReplaceAllString(text, "")
}
