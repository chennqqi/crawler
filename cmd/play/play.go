package main

import (
	"fmt"

	"github.com/champkeh/crawler/proxy/fetcher"
)

func main() {
	ip, err := fetcher.VerifyProxy("95.105.13.48:53281")
	if err != nil {
		fmt.Println("失败")
	} else {
		fmt.Println(ip)
	}
}
