package verifier

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/champkeh/crawler/proxy/types"
)

// VerifyProxy function verify the proxy available using http://httpbin.org/ip service
func VerifyProxy(ip types.ProxyIP) error {
	request, _ := http.NewRequest("GET", "http://httpbin.org/ip", nil)

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

	respBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}

	var origin types.OriginIP
	err = json.Unmarshal(respBytes, &origin)
	if err != nil {
		return err
	}

	if strings.Contains(origin.Origin, ip.IP) {
		return nil
	} else {
		return errors.New(fmt.Sprintf("代理ip设置失败: proxy:%s  origin:%s", ip.IP, origin.Origin))
	}
}
