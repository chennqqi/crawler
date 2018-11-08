package main

import (
	"fmt"
	"time"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/proxy/fetcher"
	"github.com/champkeh/crawler/utils"
	"github.com/go-redis/redis"
)

const (
	capacity = 20000
	logfile  = "redis.info.log"
)

// proxy fetcher
// 代理获取器
// 该程序不断的从 dec.ip3366.net 网站提取代理ip
// 并放入redis缓存中
// 缓存容量100,000
func main() {

	utils.MustExist(logfile)

	client := redis.NewClient(&redis.Options{
		Addr:     config.RedisAddress,
		Password: config.RedisPass,
		DB:       config.RedisDB,
	})
	defer client.Close()

	anonymous := 0
	for {
		anonymous++
		anonymous %= 3

		ips, err := fetcher.FetchProxy(anonymous)
		if err != nil {
			utils.AppendToFile(logfile,
				fmt.Sprintf("[%s]fetcher: fetch proxy ip error:%q\n",
					time.Now().Format("2006-01-02 15:04:05"), err))
			continue
		}

		for _, ip := range ips {
			_, err = client.RPush("proxyip_verify", ip.String()).Result()
			if err != nil {
				utils.AppendToFile(logfile,
					fmt.Sprintf("[%s]redis: rpush cmd error:%q\n",
						time.Now().Format("2006-01-02 15:04:05"), err))
				panic(err)
			}
		}

		_, err = client.LTrim("proxyip_verify", -1*capacity, -1).Result()
		if err != nil {
			utils.AppendToFile(logfile,
				fmt.Sprintf("[%s]redis: ltrim cmd error:%q\n",
					time.Now().Format("2006-01-02 15:04:05"), err))
			panic(err)
		}
	}
}
