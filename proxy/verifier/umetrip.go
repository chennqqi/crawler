package verifier

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"io/ioutil"

	"github.com/champkeh/crawler/proxy/types"
)

func UmetripVerify(ip types.ProxyIP) error {
	request, _ := http.NewRequest("GET", "http://www.umetrip.com", nil)

	request.Header.Set("User-Agent", GetAgent())
	request.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	request.Header.Set("Connection", "keep-alive")
	request.Header.Set("Proxy-Connection", "keep-alive")

	proxy, err := url.Parse(fmt.Sprintf("http://%s:%d", ip.IP, ip.Port))
	if err != nil {
		panic(err)
	}

	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxy),
		},
		Timeout: time.Duration(20 * time.Second),
	}

	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return errors.New("status code is not 200")
	}
	respBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", respBytes)
	return nil
}
