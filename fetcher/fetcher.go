package fetcher

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

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
