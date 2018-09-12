package scheduler

import (
	"errors"

	"github.com/champkeh/crawler/types"
)

type SimpleScheduler struct {
	requestChan chan types.Request
}

func (s *SimpleScheduler) ConfigureRequestChan(channel chan types.Request) {
	s.requestChan = channel
}

func (s *SimpleScheduler) Submit(req types.Request) {
	if s.requestChan == nil {
		panic(errors.New("before submit request to scheduler, you must" +
			" configure the request-channel for this scheduler"))
	}
	go func() {
		s.requestChan <- req
	}()
}
