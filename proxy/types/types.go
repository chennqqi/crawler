package types

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

// ProxyIP 是代理ip的提取结果
type ProxyIP struct {
	IP   string `json:"Ip"`
	Port int    `json:"Port"`
}

func (ip ProxyIP) String() string {
	return fmt.Sprintf("%s:%d", ip.IP, ip.Port)
}

func Parse(s string) ProxyIP {
	split := strings.Split(s, ":")
	if len(split) != 2 {
		log.Panic("proxyip parse err")
	}
	port, err := strconv.Atoi(split[1])
	if err != nil {
		log.Panic("proxyip parse err")
	}
	return ProxyIP{
		IP:   split[0],
		Port: port,
	}
}

type ProxyPool interface {
	Start()
	Submit(ProxyIP)
	Fetch() ProxyIP
}
