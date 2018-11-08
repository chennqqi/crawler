package fetcher

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	url2 "net/url"

	"time"

	"github.com/champkeh/crawler/proxy/pool"
	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/utils"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// Fetch 底层fetch函数
func Fetch(url string, referer string, encoding encoding.Encoding, rateLimiter types.RateLimiter) ([]byte, error) {
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

	if encoding != unicode.UTF8 {
		return ioutil.ReadAll(transform.NewReader(resp.Body, encoding.NewDecoder()))
	} else {
		return ioutil.ReadAll(resp.Body)
	}
}

func FetchRequest(req types.Request, rateLimiter types.RateLimiter) (types.ParseResult, error) {
	body, err := Fetch(req.Url, req.Referer, req.Encoding, rateLimiter)
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

func FetchWithProxy(url string, referer string, encoding encoding.Encoding, proxyPool *pool.LocalProxyPool,
	rateLimiter types.RateLimiter) ([]byte, error) {
	// limit fetch rate
	if rateLimiter != nil {
		rateLimiter.Wait()
	}

	request, _ := http.NewRequest("GET", url, nil)

	request.Header.Set("User-Agent", utils.GetAgent())
	request.Header.Set("Referer", referer)
	request.Header.Set("Connection", "keep-alive")

	// fetch a proxy from pool
	proxyip := proxyPool.Fetch()
	proxy, err := url2.Parse(fmt.Sprintf("http://%s", proxyip))
	if err != nil {
		panic(err)
	}
	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxy),
		},
		Timeout: time.Duration(60 * time.Second),
	}

	resp, err := client.Do(request)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("fetch %s error: %s", url, err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(fmt.Sprintf("fetch %s with proxy %s got wrong status code:%d", url, proxyip, resp.StatusCode))
	}
	go func() {
		// 到这一步，说明代理可以使用，则把该代理重新放回pool中
		proxyPool.Submit(proxyip)
	}()

	if encoding != unicode.UTF8 {
		return ioutil.ReadAll(transform.NewReader(resp.Body, encoding.NewDecoder()))
	} else {
		return ioutil.ReadAll(resp.Body)
	}
}

func FetchRequestWithProxy(req types.Request, proxySource *pool.LocalProxyPool,
	rateLimiter types.RateLimiter) (types.ParseResult, error) {
	body, err := FetchWithProxy(req.Url, req.Referer, req.Encoding, proxySource, rateLimiter)
	if err != nil {
		return types.ParseResult{}, errors.New(fmt.Sprintf("fetch error: %s", err))
	}

	result, err := req.ParserFunc(body)
	if err != nil {
		return types.ParseResult{}, err
	}

	result.Request = req
	return result, nil
}
