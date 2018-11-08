package pool

import (
	"sync"

	"time"

	"fmt"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/proxy/types"
	"github.com/champkeh/crawler/utils"
	"github.com/go-redis/redis"
)

type LocalProxyPool struct {
	container chan types.ProxyIP
	running   bool
	sync.Mutex
}

func (pool *LocalProxyPool) Start(capacity int, rediskey string, logfile string) {
	pool.Lock()
	defer pool.Unlock()

	if pool.running {
		return
	}

	if pool.container == nil {
		pool.container = make(chan types.ProxyIP, capacity)
	}
	pool.running = true

	go func() {
		// 连接Redis
		client := redis.NewClient(&redis.Options{
			Addr:     config.RedisAddress,
			Password: config.RedisPass,
			DB:       config.RedisDB,
		})
		defer client.Close()

		for {
			//ips, err := client.SMembers(rediskey).Result()
			//if err != nil {
			//	utils.AppendToFile(logfile,
			//		fmt.Sprintf("[%s]redis: smembers cmd error:%q\n",
			//			time.Now().Format("2006-01-02 15:04:05"), err))
			//	continue
			//}
			ips, err := client.LRange("proxyip_verify", 0, 50).Result()
			if err != nil {
				utils.AppendToFile(logfile,
					fmt.Sprintf("[%s]redis: lrange cmd error:%q\n",
						time.Now().Format("2006-01-02 15:04:05"), err))
				continue
			}
			result, err := client.LTrim("proxyip_verify", 50, -1).Result()
			if err != nil {
				utils.AppendToFile(logfile,
					fmt.Sprintf("[%s]redis: ltrim cmd error:%q\n",
						time.Now().Format("2006-01-02 15:04:05"), err))
				continue
			}
			if result != "OK" {
				utils.AppendToFile(logfile,
					fmt.Sprintf("[%s]redis: ltrim cmd result:%q\n",
						time.Now().Format("2006-01-02 15:04:05"), result))
				continue
			}

			for _, ip := range ips {
				pool.container <- types.Parse(ip)
			}

			if len(pool.container) >= (capacity - 200) {
				time.Sleep(60 * time.Minute)
			} else {
				time.Sleep(20 * time.Second)
			}
		}
	}()
}

func (pool *LocalProxyPool) Submit(proxy types.ProxyIP) {
	for pool.container == nil {
		time.Sleep(5 * time.Second)
	}
	go func() {
		pool.container <- proxy
	}()
}

func (pool *LocalProxyPool) Fetch() types.ProxyIP {
	return <-pool.container
}

func (pool *LocalProxyPool) Count() int {
	return len(pool.container)
}
