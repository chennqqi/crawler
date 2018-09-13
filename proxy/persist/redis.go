package persist

import (
	"log"

	"github.com/champkeh/crawler/config"
	"github.com/champkeh/crawler/proxy/types"
	"github.com/garyburd/redigo/redis"
)

var (
	conn redis.Conn
)

func init() {
	var err error
	// 连接Redis
	conn, err = redis.Dial("tcp", config.Redis_addr,
		redis.DialDatabase(config.Redis_db),
		redis.DialPassword(config.Redis_auth))
	if err != nil {
		panic(err)
	}
}

func Save(proxy types.ProxyIP) error {
	// 写入redis
	_, err := conn.Do("rpush", "proxyip_list", proxy.String())
	_, err = conn.Do("ltrim", "proxyip_list", -1*config.Redis_capacity, -1)
	if err != nil {
		log.Println("rpush err: ", err)
		return err
	}
	return nil
}
