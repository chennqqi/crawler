package types

import "time"

// Request
type Request struct {
	Dep        string
	Arr        string
	Date       string
	Url        string
	ParserFunc func([]byte) ParseResult
}

// ParseResult
type ParseResult struct {
	Dep   string
	Arr   string
	Date  string
	Items []interface{}
}

func NilParser(contents []byte) ParseResult {
	return ParseResult{}
}

type Airport struct {
	DepCode string
	ArrCode string
}

func init() {
	T1 = time.Now()
}

var (
	T1 time.Time
)
