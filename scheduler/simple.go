package scheduler

import (
	"github.com/champkeh/crawler/types"
)

type SimpleScheduler struct {
	requestChan chan types.Request
}

func (s *SimpleScheduler) ConfigureRequestChan(channel chan types.Request) {
	s.requestChan = channel
}

func (s *SimpleScheduler) Submit(request types.Request) {
	go func() {
		s.requestChan <- request
	}()
}
