package pool

import (
	"sync"

	"time"

	"fmt"

	"github.com/champkeh/crawler/proxy/fetcher"
	"github.com/champkeh/crawler/proxy/types"
)

type LocalProxyPool struct {
	container chan types.ProxyIP
	running   bool
	sync.Mutex
}

func (pool *LocalProxyPool) Start() {
	pool.Lock()
	defer pool.Unlock()

	if pool.running {
		return
	}

	if pool.container == nil {
		pool.container = make(chan types.ProxyIP, 3000)
	}
	pool.running = true

	go func() {
		anonymousLevel := 0
		for {
			anonymousLevel++
			anonymousLevel %= 3

			ips, err := fetcher.FetchProxy(anonymousLevel)
			if err != nil {
				fmt.Println(err)
				continue
			}

			for _, ip := range ips {
				pool.container <- ip
			}
		}
	}()
}

func (pool *LocalProxyPool) Submit(proxy types.ProxyIP) {
	for pool.container == nil {
		time.Sleep(100 * time.Millisecond)
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
