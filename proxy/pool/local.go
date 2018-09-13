package pool

import (
	"sync"

	"time"

	"github.com/champkeh/crawler/proxy/types"
)

type LocalProxyPool struct {
	pool    chan types.ProxyIP
	running bool
	sync.Mutex
}

func (pool *LocalProxyPool) Start() {
	pool.Lock()
	defer pool.Unlock()

	if pool.running {
		return
	}

	if pool.pool == nil {
		pool.pool = make(chan types.ProxyIP, 300)
	}
	pool.running = true
}

func (pool *LocalProxyPool) Submit(proxy types.ProxyIP) {
	for pool.pool == nil {
		time.Sleep(100 * time.Millisecond)
	}
	go func() {
		pool.pool <- proxy
	}()
}

func (pool *LocalProxyPool) Fetch() types.ProxyIP {
	return <-pool.pool
}
