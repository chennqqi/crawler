package parser

import (
	"bytes"
	"errors"

	"strings"

	"regexp"

	"github.com/PuerkitoBio/goquery"
	"github.com/champkeh/crawler/types"
)

type FlightDetailData struct {
	FlightNo         string
	FlightDate       string
	DepCity          string
	ArrCity          string
	FlightState      string
	DepPlanTime      string
	DepActualTime    string
	ArrPlanTime      string
	ArrActualTime    string
	Mileage          string
	Duration         string
	DepWeather       string
	ArrWeather       string
	CheckinCounter   string //值机柜台
	BoardGate        string //登机口
	BoardType        string //等级方式
	BaggageTurntable string //行李转盘
}

func ParseDetail(contents []byte) (types.ParseResult, error) {
	reader := bytes.NewReader(contents)
	doc, err := goquery.NewDocumentFromReader(reader)
	if err != nil {
		panic(err)
	}
	result := types.ParseResult{}

	infoBoxes := doc.Find(".detail-box .detail-info")
	if infoBoxes.Length() <= 0 {
		return result, nil
	} else {
		details := parseMultiFlight(doc, infoBoxes.Length())
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
	tit := doc.Find(".detail-t").First()
	fno := strings.TrimSpace(tit.Find("strong.ml5").Text())
	date := strings.TrimSpace(tit.Find("span.ml10").Text())

	flyBoxes := doc.Find(".detail-info")

	for i := 0; i < count; i++ {
		first := flyBoxes.Eq(i)
		state := strings.TrimSpace(first.Find(".detail-m .detail-fly").First().Find(".between i").Text())
		for j := i; j < count; j++ {
			second := flyBoxes.Eq(j)

			if stateIsEnd(state) && j > i {
				si := i + 1
				state = strings.TrimSpace(flyBoxes.Eq(si).Find(".detail-m .detail-fly").First().Find(".between i").Text())
				for stateIsEnd(state) && si < j {
					si++
					state = strings.TrimSpace(flyBoxes.Eq(si).Find(".detail-m .detail-fly").First().Find(".between i").Text())
				}
			}

			// 计算航段时长 从 i 到 j-1
			duration := "0小时0分"
			mileage := "0公里"

			// 起飞时间的位置比较特殊
			depActualTime := ParseTime(first.Find(".detail-m .detail-fly").First().Find(".departure").Find("p.time").Text())
			depPlanTime := ParseTime(first.Find(".detail-m .detail-fly").First().Find(".departure").Find("p.gray").Text())
			depCity := ParseCity(first.Find(".detail-m .detail-fly").Last().Find(".departure").Find("p").Text())
			depWeather := first.Find(".detail-m .detail-fly").Last().Find(".departure").Find("span").Text()

			arrActualTime := ParseTime(second.Find(".detail-m .detail-fly").First().Find(".arrive").Find("p.time").Text())
			arrPlanTime := ParseTime(second.Find(".detail-m .detail-fly").First().Find(".arrive").Find("p.gray").Text())
			arrCity := ParseCity(second.Find(".detail-m .detail-fly").Last().Find(".arrive").Find("p").Text())
			arrWeather := second.Find(".detail-m .detail-fly").Last().Find(".arrive").Find("span").Text()

			checkinCounter := trimSpace(first.Find(".operation .item").Eq(0).Find(".m").Text())
			boardGate := trimSpace(first.Find(".operation .item").Eq(1).Find(".m").Find("div").Eq(0).Text())
			boardType := trimSpace(first.Find(".operation .item").Eq(1).Find(".m").Find("div").Eq(1).Text())
			baggageTurntable := trimSpace(first.Find(".operation .item").Eq(2).Find(".m").Text())

			detail := FlightDetailData{
				FlightNo:         fno,
				FlightDate:       date,
				FlightState:      state,
				Duration:         duration,
				Mileage:          mileage,
				DepCity:          depCity,
				DepWeather:       depWeather,
				DepPlanTime:      depPlanTime,
				DepActualTime:    depActualTime,
				ArrCity:          arrCity,
				ArrWeather:       arrWeather,
				ArrPlanTime:      arrPlanTime,
				ArrActualTime:    arrActualTime,
				CheckinCounter:   checkinCounter,
				BoardGate:        boardGate,
				BoardType:        boardType,
				BaggageTurntable: baggageTurntable,
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
		return 1
	}
	return f(n-1) + n
}

func stateIsEnd(state string) bool {
	if strings.Contains(state, "到达") ||
		strings.Contains(state, "取消") ||
		strings.Contains(state, "备降") ||
		strings.Contains(state, "返航") {
		return true
	}

	return false
}

func ParseCity(raw string) string {
	var re = `(.+)\n`
	match := regexp.MustCompile(re).FindStringSubmatch(raw)

	if len(match) <= 1 {
		return ""
	}

	return match[1]
}

func ParseTime(mask string) string {
	if mask == "" {
		return ""
	}

	var re = `(\d{2}:\d{2})`
	match := regexp.MustCompile(re).FindStringSubmatch(mask)

	if len(match) <= 1 {
		return ""
	}

	return match[1]
}
func trimSpace(text string) string {
	if text == "" {
		return ""
	}

	reg := regexp.MustCompile(`[\n\t ]`)
	return reg.ReplaceAllString(text, "")
}
