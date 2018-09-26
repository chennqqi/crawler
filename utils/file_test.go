package utils

import "testing"

func TestAppendToFile(t *testing.T) {
	AppendToFile("file.log", "hello")
}
