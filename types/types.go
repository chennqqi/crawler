package types

// Request
type Request struct {
	Url        string
	ParserFunc func([]byte) ParseResult
}

// ParseResult
type ParseResult struct {
	Items []interface{}
}

func NilParser(contents []byte) ParseResult {
	return ParseResult{}
}

type Airport struct {
	DepCode string
	ArrCode string
}
