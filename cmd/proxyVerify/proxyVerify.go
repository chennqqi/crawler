package main

import (
	"fmt"
	"time"

	"sync"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/proxy/fetcher"
	"github.com/champkeh/crawler/utils"
	"github.com/go-redis/redis"
)

const (
	logfile = "redis.info.log"
)

// proxy verify
// 代理验证器
// 该程序不断的对 redis 中的代理ip进行验证
// 验证可用的代理放入 redis 缓存中
// 缓存容量暂不设限
func main() {

	utils.MustExist(logfile)

	client := redis.NewClient(&redis.Options{
		Addr:     config.RedisAddress,
		Password: config.RedisPass,
		DB:       config.RedisDB,
	})
	defer client.Close()

	for {
		ips, err := client.LRange("proxyip_list", 0, 500).Result()
		if err != nil {
			utils.AppendToFile(logfile,
				fmt.Sprintf("[%s]redis: lrange cmd error:%q\n",
					time.Now().Format("2006-01-02 15:04:05"), err))
			continue
		}
		_, err = client.LTrim("proxyip_list", 500, -1).Result()
		if err != nil {
			utils.AppendToFile(logfile,
				fmt.Sprintf("[%s]redis: ltrim cmd error:%q\n",
					time.Now().Format("2006-01-02 15:04:05"), err))
			continue
		}

		tokens := make(chan struct{}, 100)
		wg := sync.WaitGroup{}

		for _, ip := range ips {
			ip := ip
			wg.Add(1)
			tokens <- struct{}{}

			go func() {
				defer func() {
					<-tokens
					wg.Done()
				}()

				_, err = fetcher.VerifyProxy(ip)
				if err != nil {
					return
				}

				//_, err = client.SAdd("proxyip_verify", ip).Result()
				_, err = client.RPush("proxyip_verify", ip).Result()
				if err != nil {
					utils.AppendToFile(logfile,
						fmt.Sprintf("[%s]redis: rpush cmd error:%q\n",
							time.Now().Format("2006-01-02 15:04:05"), err))
					return
				}
			}()
		}

		wg.Wait()
	}
}
