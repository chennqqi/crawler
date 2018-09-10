package scheduler

import (
	"github.com/champkeh/crawler/engine"
)

type QueuedScheduler struct {
}

func (s QueuedScheduler) Submit(engine.Request) {
	panic("implement me")
}

func (s QueuedScheduler) ConfigureMasterWorkerChan(chan engine.Request) {
	panic("implement me")
}
