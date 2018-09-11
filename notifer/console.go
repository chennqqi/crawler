package notifer

import (
	"fmt"

	"github.com/champkeh/crawler/types"
)

type ConsolePrintNotifier struct {
	printChan chan types.NotifyData
}

func (o *ConsolePrintNotifier) ConfigureChan(channel chan types.NotifyData) {
	o.printChan = channel
}

func (o *ConsolePrintNotifier) Print(data types.NotifyData) {
	go func() {
		o.printChan <- data
	}()
}

func (o *ConsolePrintNotifier) Run() {
	for {
		notify := <-o.printChan
		fmt.Printf("\r%s", notify)
	}
}
