package fetcher

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"strconv"

	"time"

	"fmt"
	"net/url"
	"strings"

	"github.com/champkeh/crawler/proxy/types"
	"github.com/champkeh/crawler/utils"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

// limit the fetch proxy api call rate to 1/6 cps
var rateLimiter = time.Tick(6 * time.Second)

// FetchProxy function fetch the proxy ip list from ip3366 proxy-ip provider
// result list contains 20 records at most.
//
// anonymous is level:
//   0 - normal
//   1 - advance
//   2 - super advance
func FetchProxy(anonymous int) ([]types.ProxyIP, error) {
	<-rateLimiter

	apiurl := "http://dec.ip3366.net/api/"
	apiurl += "?key=20180412114150939"
	apiurl += "&anonymoustype=" + strconv.Itoa(anonymous+2) // 匿名级别 普通匿名2 高级匿名3 超级匿名4
	apiurl += "&getnum=20"                                  // 一次最多提取20
	apiurl += "&proxytype=0"                                // 代理类型 http
	apiurl += "&formats=2"                                  // 返回json格式

	resp, err := http.Get(apiurl)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	utf8Reader := transform.NewReader(resp.Body, simplifiedchinese.GBK.NewDecoder())
	respBytes, err := ioutil.ReadAll(utf8Reader)
	if err != nil {
		return nil, err
	}

	var proxy []types.ProxyIP
	err = json.Unmarshal(respBytes, &proxy)
	if err != nil {
		return nil, err
	}
	return proxy, nil
}

// 验证代理ip的可用性
func VerifyProxy(ip string) (string, error) {
	request, _ := http.NewRequest("GET", "http://httpbin.org/ip", nil)

	request.Header.Set("User-Agent", utils.GetAgent())
	request.Header.Set("Connection", "keep-alive")
	request.Header.Set("Proxy-Connection", "keep-alive")

	proxy, err := url.Parse(fmt.Sprintf("http://%s", ip))
	if err != nil {
		panic(err)
	}

	client := &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxy),
		},
		Timeout: time.Duration(40 * time.Second),
	}

	response, err := client.Do(request)
	if err != nil {
		return ip, err
	}
	defer response.Body.Close()

	respBytes, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return ip, err
	}
	var origin OriginIP
	err = json.Unmarshal(respBytes, &origin)
	if err != nil {
		return ip, err
	}

	if strings.Contains(origin.Origin, strings.Split(ip, ":")[0]) {
		return ip, nil
	} else {
		return ip, errors.New(fmt.Sprintf("代理ip设置失败: proxy:%s  origin:%s", ip, origin.Origin))
	}
}

// OriginIP 是使用 http://httpbin.org/ip 进行代理ip验证的结果
type OriginIP struct {
	Origin string `json:"origin"`
}
