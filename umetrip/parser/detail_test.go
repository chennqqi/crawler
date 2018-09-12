package parser

import (
	"fmt"
	"io/ioutil"
	"testing"
)

func TestParseDetail(t *testing.T) {
	contents, err := ioutil.ReadFile("./testdata/MU5137_2018-09-12.html")
	if err != nil {
		t.Errorf("read test data fail: %v", err)
	}

	parseResult := ParseDetail(contents)
	fmt.Println(parseResult)
}
