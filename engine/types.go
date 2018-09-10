package engine

// Request
type Request struct {
	Url        string
	ParserFunc func([]byte) ParseResult
}

// ParseResult
type ParseResult struct {
	Requests []Request
	Items    []interface{}
}

func NilParser(contents []byte) ParseResult {
	return ParseResult{}
}
