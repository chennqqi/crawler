package parser

import (
	"fmt"
	"io/ioutil"
	"testing"
)

func TestSingleParseDetail(t *testing.T) {
	contents, err := ioutil.ReadFile("./testdata/MU5101_2018-09-14.html")
	if err != nil {
		t.Errorf("read test data fail: %v", err)
	}

	parseResult, err := ParseDetail(contents)
	for _, result := range parseResult.Items {
		fmt.Println(result)
	}
}

func TestParseMultiDetail(t *testing.T) {
	contents, err := ioutil.ReadFile("./testdata/MU5696_2018-09-14.html")
	if err != nil {
		t.Errorf("read test data fail: %v", err)
	}

	parseResult, err := ParseDetail(contents)
	for _, result := range parseResult.Items {
		fmt.Println(result)
	}
}

func TestParseSuperMultiDetail(t *testing.T) {
	contents, err := ioutil.ReadFile("./testdata/ZH9746_2018-09-22.html")
	if err != nil {
		t.Errorf("read test data fail: %v", err)
	}

	parseResult, err := ParseDetail(contents)
	for _, result := range parseResult.Items {
		fmt.Println(result)
	}
}

func TestNotFound(t *testing.T) {
	contents, err := ioutil.ReadFile("./testdata/notfound.html")
	if err != nil {
		t.Errorf("read test data fail: %v", err)
	}

	parseResult, err := ParseDetail(contents)
	if err != nil {
		t.Errorf("parse error: %v", err)
	}
	for _, result := range parseResult.Items {
		fmt.Println(result)
	}
}

func TestParseTime(t *testing.T) {
	cases := []struct {
		url      string
		expected string
	}{
		{
			url:      "http://www.umetrip.com/mskyweb/graphic.do?str=to/N9MNwYbM44jK4dXvDhw==&width=60&height=25&front=156,156,156&back=248,248,248&size=20&xpos=0&ypos=20",
			expected: "to/N9MNwYbM44jK4dXvDhw==",
		},
		{
			url:      "http://www.umetrip.com/mskyweb/graphic.do?str=/m0dNEqrs639gdW4KbtRIA==&width=60&height=25&front=68,68,68&back=248,248,248&size=20&xpos=0&ypos=20",
			expected: "/m0dNEqrs639gdW4KbtRIA==",
		},
		{
			url:      "http://www.umetrip.com/mskyweb/graphic.do?str=RjgQwQ0X5HoRDRPEtFkxUw==&width=60&height=17&front=68,68,68&back=248,248,248&size=16&xpos=0&ypos=12",
			expected: "RjgQwQ0X5HoRDRPEtFkxUw==",
		},
		{
			url:      "http://www.umetrip.com/mskyweb/graphic.do?str=RjgQwQ0X5HoRDRPEtFkxUw==&width=60&height=17&front=68,68,68&back=248,248,248&size=16&xpos=0&ypos=12",
			expected: "RjgQwQ0X5HoRDRPEtFkxUw==",
		},
	}

	for _, c := range cases {
		actual := ParseTime(c.url)
		if actual != c.expected {
			t.Errorf("got %s; expected %s", actual, c.expected)
		}
	}
}

func TestParseCode(t *testing.T) {
	raw := `
										上海
										
										(
										SHA
										)
									`
	code := ParseCityCode(raw)
	fmt.Println(code)
	code = ParseCityCode("")
	fmt.Println(code)
}

func TestParsePreFlightInfo(t *testing.T) {
	text := "前序航班MU5124[PEK到SHA]，已于20:55到达"
	info := ParsePreFlightInfo(text)
	fmt.Println(info)
}

func TestF(t *testing.T) {
	for i := 1; i <= 10; i++ {
		fmt.Printf("f(%d)=%d\n", i, f(i))
	}
}

func TestParseMultiFlight(t *testing.T) {
	contents, err := ioutil.ReadFile("./testdata/MU5696_2018-09-30.html")
	if err != nil {
		t.Errorf("read test data fail: %v", err)
	}

	parseResult, err := ParseDetail(contents)
	if err != nil {
		t.Errorf("parse error: %v", err)
	}
	for _, result := range parseResult.Items {
		fmt.Println(result)
	}
}

func TestParseDuration(t *testing.T) {
	fmt.Println(ParseDuration("1小时20分"))
	fmt.Println(ParseDuration("1小时45分"))
	fmt.Println(ParseDuration("1小时50分"))
	fmt.Println(ParseDuration("2小时5分"))
	fmt.Println(ParseDuration("2小时0分"))
	fmt.Println(ParseDuration("50分"))
}

func TestAddDuration(t *testing.T) {
	fmt.Println(addduration("20分", "1小时0分"))
	fmt.Println(addduration("1小时20分", "20分"))
	fmt.Println(addduration("1小时0分", "3小时59分"))
	fmt.Println(addduration("2小时20分", "1小时59分"))
	fmt.Println(addduration("20分", "40分"))
	fmt.Println(addduration("20分", "30分"))
}
