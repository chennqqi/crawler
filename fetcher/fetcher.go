package fetcher

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	url2 "net/url"

	"time"

	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/utils"
)

func Fetch(url string, referer string, rateLimiter types.RateLimiter) ([]byte, error) {
	// limit fetch rate
	if rateLimiter != nil {
		rateLimiter.Wait()
	}

	request, _ := http.NewRequest("GET", url, nil)

	request.Header.Set("User-Agent", utils.GetAgent())
	request.Header.Set("Referer", referer)
	request.Header.Set("Connection", "keep-alive")

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("fetch %s error: %s", url, err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(fmt.Sprintf("fetch %s got wrong status code:%d", url, resp.StatusCode))
	}

	return ioutil.ReadAll(resp.Body)
}

func FetchWorker(req types.Request, rateLimiter types.RateLimiter) (types.ParseResult, error) {
	body, err := Fetch(req.Url, "http://www.baidu.com/", rateLimiter)
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

func FetchWithProxy(url string, proxyip string, rateLimiter types.RateLimiter) ([]byte, error) {
	// limit fetch rate
	if rateLimiter != nil {
		rateLimiter.Wait()
	}

	request, _ := http.NewRequest("GET", url, nil)

	request.Header.Set("User-Agent", utils.GetAgent())
	request.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	request.Header.Set("Referer", "https://www.baidu.com/")
	request.Header.Set("Connection", "keep-alive")
	request.Header.Set("Proxy-Connection", "keep-alive")

	proxy, err := url2.Parse(fmt.Sprintf("http://%s", proxyip))
	if err != nil {
		panic(err)
	}
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxy),
		},
		Timeout: time.Duration(30 * time.Second),
	}

	resp, err := client.Do(request)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("fetch %s error: %s", url, err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(fmt.Sprintf("http: wrong status code:%d", resp.StatusCode))
	}

	return ioutil.ReadAll(resp.Body)
}
