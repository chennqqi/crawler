package fetcher

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/champkeh/crawler/proxy/verifier"
	"github.com/champkeh/crawler/types"
)

func Fetch(url string, rateLimiter types.RateLimiter) ([]byte, error) {
	// limit fetch rate
	if rateLimiter != nil {
		rateLimiter.Wait()
	}

	request, _ := http.NewRequest("GET", url, nil)

	request.Header.Set("User-Agent", verifier.GetAgent())
	request.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	request.Header.Set("Connection", "keep-alive")

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("fetch %s error: %s", url, err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(fmt.Sprintf("http: wrong status code:%d", resp.StatusCode))
	}

	return ioutil.ReadAll(resp.Body)
}

func FetchWorker(req types.Request, rateLimiter types.RateLimiter) (types.ParseResult, error) {
	body, err := Fetch(req.Url, rateLimiter)
	if err != nil {
		return types.ParseResult{}, errors.New(fmt.Sprintf("fetch error: %s", err))
	}

	result, err := req.ParserFunc(body)
	if err != nil {
		return types.ParseResult{}, errors.New(fmt.Sprintf("parse (%s:%s) error: %s", req.RawParam.Date, req.RawParam.Fno, err))
	}

	result.Request = req
	return result, nil
}
