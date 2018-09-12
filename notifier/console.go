package notifier

import (
	"fmt"

	"sync"

	"time"

	"github.com/champkeh/crawler/types"
)

type ConsolePrintNotifier struct {
	printChan chan types.NotifyData
	running   bool
	sync.Mutex
}

func (o *ConsolePrintNotifier) Print(data types.NotifyData) {
	for o.printChan == nil {
		time.Sleep(100 * time.Millisecond)
	}

	go func() {
		o.printChan <- data
	}()
}

func (o *ConsolePrintNotifier) Run() {
	o.Lock()

	if o.running {
		o.Unlock()
		return
	}
	o.running = true

	if o.printChan == nil {
		o.printChan = make(chan types.NotifyData, 100)
	}
	o.Unlock()

	for {
		notify := <-o.printChan
		fmt.Printf("\r%s", notify)
	}
}
