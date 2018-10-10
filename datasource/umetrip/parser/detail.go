package parser

import (
	"bytes"

	"strings"

	"regexp"

	"errors"

	"fmt"

	"strconv"

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
	DepActualTime    string
	ArrPlanTime      string
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

func ParseDetail(contents []byte) (types.ParseResult, error) {
	reader := bytes.NewReader(contents)
	doc, err := goquery.NewDocumentFromReader(reader)
	if err != nil {
		panic(err)
	}
	result := types.ParseResult{}

	flyBoxes := doc.Find(".fly_box")
	if flyBoxes.Length() <= 0 {
		return result, nil
	} else if flyBoxes.Length() == 1 {
		return result, errors.New(fmt.Sprintf("解析该航班出错: fly_box数量异常: %d", flyBoxes.Length()))
	} else {
		details := parseMultiFlight(doc, flyBoxes.Length())
		for _, d := range details {
			result.Items = append(result.Items, d)
		}
	}

	return result, nil
}

func parseMultiFlight(doc *goquery.Document, count int) []FlightDetailData {
	//state:3   count:4   n:6
	details := make([]FlightDetailData, 0, f(count))

	// 提前解析出航班号
	tit := doc.Find(".tit")
	fno := strings.TrimSpace(tit.Find("span b").Text())

	p_info := doc.Find(".p_info")

	flyBoxes := doc.Find(".fly_box")

	for i := 0; i < count-1; i++ {
		first := flyBoxes.Eq(i)
		state := strings.TrimSpace(tit.Find("div.state div").Eq(i).Text())
		for j := i + 1; j < count; j++ {
			second := flyBoxes.Eq(j)

			if stateIsEnd(state) && j-i > 1 {
				si := i + 1
				state = strings.TrimSpace(tit.Find("div.state div").Eq(si).Text())
				for stateIsEnd(state) && si < (j-1) {
					si++
					state = strings.TrimSpace(tit.Find("div.state div").Eq(si).Text())
				}
			}

			// 计算航段时长 从 i 到 j-1
			duration := "0小时0分"
			for di := i; di < j; di++ {
				duration1 := strings.TrimSpace(p_info.Find("ul").Eq(di).Find(".time span").Text())
				duration = addduration(duration, duration1)
			}

			// 起飞时间的位置比较特殊
			depplantime := ""
			depactualtime := ""
			if i == 0 {
				depplantime = ParseTime(first.Find(".f_com .f_m .time dl").Eq(0).Find("img").AttrOr("src", ""))
				depactualtime = ParseTime(first.Find(".f_com .f_m .time dl").Eq(1).Find("img").AttrOr("src", ""))
			} else {
				depplantime = ParseTime(first.Find(".f_com .f_m .time dl").Eq(2).Find("img").AttrOr("src", ""))
				depactualtime = ParseTime(first.Find(".f_com .f_m .time dl").Eq(3).Find("img").AttrOr("src", ""))
			}

			firstCityCode := ParseCityCode(strings.TrimSpace(first.Find(".f_tit h2").Text()))
			secondCityCode := ParseCityCode(strings.TrimSpace(second.Find(".f_tit h2").Text()))
			if firstCityCode == nil || secondCityCode == nil {
				fmt.Printf("\nwarn: CityCode = nil [%s:%s]\n", ParseDate(first.Find(".f_tit span").Text()), fno)
				continue
			}

			detail := FlightDetailData{
				FlightNo:      fno,
				FlightDate:    ParseDate(first.Find(".f_tit span").Text()),
				FlightState:   state,
				Mileage:       strings.TrimSpace(p_info.Find("ul").Eq(i).Find(".mileage span").Text()),
				Duration:      duration,
				Age:           strings.TrimSpace(p_info.Find("ul").Eq(i).Find(".age span").Text()),
				DepCity:       firstCityCode[0],
				DepCode:       firstCityCode[1],
				DepWeather:    trimSpace(first.Find(".f_com .f_r p").Eq(0).Text()),
				DepFlow:       trimSpace(first.Find(".f_com .f_r p").Eq(2).Text()),
				DepPlanTime:   depplantime,
				DepActualTime: depactualtime,
				ArrCity:       secondCityCode[0],
				ArrCode:       secondCityCode[1],
				ArrWeather:    trimSpace(second.Find(".f_com .f_r p").Eq(0).Text()),
				ArrFlow:       trimSpace(second.Find(".f_com .f_r p").Eq(2).Text()),
				ArrPlanTime:   ParseTime(second.Find(".f_com .f_m .time dl").Eq(0).Find("img").AttrOr("src", "")),
				ArrActualTime: ParseTime(second.Find(".f_com .f_m .time dl").Eq(1).Find("img").AttrOr("src", "")),
			}

			// 判断是否有前序航班
			if first.Find(".f_tit div").Is("div") {
				text := first.Find(".f_tit div").Text()
				if info := ParsePreFlightInfo(text); info != nil {
					detail.PreFlightNo = info[0]
					detail.PreFlightDepCode = info[1]
					detail.PreFlightArrCode = info[2]
					detail.PreFlightState = info[3]
				}
			}
			details = append(details, detail)
		}
	}

	return details
}

func f(n int) int {
	if n <= 0 {
		panic(errors.New("n less than 0"))
	}
	if n == 1 {
		return 0
	}
	return f(n-1) + (n - 1)
}

func stateIsEnd(state string) bool {
	if state == "到达" || state == "取消" || state == "备降" || state == "返航" {
		return true
	}

	return false
}

func addduration(duration1, duration2 string) string {
	hour, min := ParseDuration(duration1)
	hour2, min2 := ParseDuration(duration2)
	if min+min2 >= 60 {
		return fmt.Sprintf("%d小时%d分", hour+hour2+(min+min2)/60, (min+min2)%60)
	} else {
		if hour+hour2 > 0 {
			return fmt.Sprintf("%d小时%d分", hour+hour2, min+min2)
		} else {
			return fmt.Sprintf("%d分", min+min2)
		}
	}
}
func ParseDuration(duratoin string) (hour, min int) {
	var re = `((\d+)小时)?(\d+)分`
	match := regexp.MustCompile(re).FindStringSubmatch(duratoin)

	if len(match) <= 0 {
		return 0, 0
	}
	hour, _ = strconv.Atoi(match[2])
	min, _ = strconv.Atoi(match[3])
	return
}

func ParseTime(mask string) string {
	if mask == "" {
		return ""
	}

	var re = `graphic\.do\?str=([^&]+)&`
	match := regexp.MustCompile(re).FindStringSubmatch(mask)

	if len(match) <= 1 {
		return ""
	}

	return match[1]
}

func ParseCityCode(raw string) []string {
	var re = `(?sU)(.+)\((.+)\)`
	match := regexp.MustCompile(re).FindStringSubmatch(raw)

	result := []string{}
	if match == nil {
		return nil
	}
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

	if len(match) <= 1 {
		return nil
	}

	return match[1:]
}

func trimSpace(text string) string {
	if text == "" {
		return ""
	}

	reg := regexp.MustCompile(`[\n\t ]`)
	return reg.ReplaceAllString(text, "")
}
