package scheduler

import (
	"github.com/champkeh/crawler/types"
)

type SimpleScheduler struct {
	workerChan chan types.Request
}

func (s *SimpleScheduler) ConfigureMasterWorkerChan(ch chan types.Request) {
	s.workerChan = ch
}

func (s *SimpleScheduler) Submit(r types.Request) {
	go func() {
		s.workerChan <- r
	}()
}
