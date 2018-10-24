package scheduler

//
//type SimpleRequestScheduler struct {
//	requestChan chan types.Request
//}
//
//func (s *SimpleRequestScheduler) ConfigureRequestChan(channel chan types.Request) {
//	s.requestChan = channel
//}
//
//func (s *SimpleRequestScheduler) Submit(req types.Request) {
//	if s.requestChan == nil {
//		panic(errors.New("before submit request to scheduler, you must" +
//			" configure the request-channel for this scheduler"))
//	}
//	go func() {
//		s.requestChan <- req
//	}()
//}
