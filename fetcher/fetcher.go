package fetcher

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"time"

	"net/url"

	types2 "github.com/champkeh/crawler/proxy/types"
	"github.com/champkeh/crawler/proxy/verifier"
	"github.com/champkeh/crawler/types"
)

func Fetch(url string, rateLimiter types.RateLimiter) ([]byte, error) {
	// limit fetch rate
	rateLimiter.Wait()

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(fmt.Sprintf("http: wrong status code:%d", resp.StatusCode))
	}

	return ioutil.ReadAll(resp.Body)
}

func FetchWithProxy(url2 string, rateLimiter types.RateLimiter, proxyIP types2.ProxyIP) ([]byte, error) {
	// limit fetch rate
	rateLimiter.Wait()

	request, _ := http.NewRequest("GET", url2, nil)

	request.Header.Set("User-Agent", verifier.GetAgent())
	request.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	request.Header.Set("Connection", "keep-alive")
	request.Header.Set("Proxy-Connection", "keep-alive")

	proxy, err := url.Parse(fmt.Sprintf("http://%s:%d", proxyIP.IP, proxyIP.Port))
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
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(fmt.Sprintf("http: wrong status code:%d", resp.StatusCode))
	}

	return ioutil.ReadAll(resp.Body)
}
