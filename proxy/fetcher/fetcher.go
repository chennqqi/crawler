package fetcher

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"strconv"

	"time"

	"github.com/champkeh/crawler/proxy/types"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

// limit the fetch proxy api call rate to 1/6 cps
var rateLimiter = time.Tick(8 * time.Second)

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
