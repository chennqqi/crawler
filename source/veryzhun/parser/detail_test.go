package parser

import (
	"testing"

	"fmt"

	"io/ioutil"
)

func TestParseDetail(t *testing.T) {
	contents, err := ioutil.ReadFile("./testdata/error.json")
	if err != nil {
		t.Fatalf("fetch error: %s", err)
	}

	result, err := ParseDetail(contents)
	if err != nil {
		t.Errorf("parse error: %s", err)
	} else {
		for _, item := range result.Items {
			fmt.Println(item)
		}
	}
}
