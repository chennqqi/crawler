package main

import (
	"github.com/champkeh/crawler/engine"
)

func main() {
	// execute once time per day.
	// should make this be a Scheduled Tasks
	engine.DefaultSimpleEngine.Run()
}
