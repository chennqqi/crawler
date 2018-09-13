package pool

import (
	"sync"

	"github.com/champkeh/crawler/proxy/types"
)

type RemoteProxyPool struct {
	pool    chan types.ProxyIP
	running bool
	sync.Mutex
}

func (pool *RemoteProxyPool) Start() {
	panic("implement me")
}

func (pool *RemoteProxyPool) Submit(types.ProxyIP) {
	panic("implement me")
}

func (pool *RemoteProxyPool) Fetch() types.ProxyIP {
	panic("implement me")
}
