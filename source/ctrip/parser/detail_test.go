package parser

import (
	"fmt"
	"io/ioutil"
	"testing"
)

func TestParseDetail(t *testing.T) {
	contents, err := ioutil.ReadFile("./testdata/MU5696_2018-10-16.html")
	if err != nil {
		t.Errorf("read test data fail: %v", err)
	}

	parseResult, err := ParseDetail(contents)
	for _, result := range parseResult.Items {
		fmt.Println(result)
	}
}

func TestParseCity(t *testing.T) {
	fmt.Println(ParseCity(`上海
                                虹桥国际机场T2`))
}

func TestParseTime(t *testing.T) {
	fmt.Println(ParseTime("计划到达 15:55"))
}
