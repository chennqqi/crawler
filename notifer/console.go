package notifer

import (
	"fmt"

	"github.com/champkeh/crawler/types"
)

type ConsoleNotifier struct {
	outputChan chan types.NotifyData
}

func (o *ConsoleNotifier) ConfigureChan(ch chan types.NotifyData) {
	o.outputChan = ch
}

func (o *ConsoleNotifier) Notify(out types.NotifyData) {
	go func() {
		o.outputChan <- out
	}()
}

func (o *ConsoleNotifier) Run() {
	for {
		notify := <-o.outputChan
		fmt.Printf("\r%s", notify)
	}
}
