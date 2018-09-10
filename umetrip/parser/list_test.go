package parser

import (
	"testing"

	"github.com/champkeh/crawler/fetcher"
)

func TestParseList(t *testing.T) {
	body, err := fetcher.Fetch("http://www.umetrip.com/mskyweb/fs/fa.do?dep=SHA&arr=PEK&date=2018-09-09")
	if err != nil {
		panic(err)
	}

	ParseList(body)

	// verify list
}
