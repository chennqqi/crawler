package utils

import (
	"fmt"
	"io"
	"os"
	"sync"
)

var mutex sync.Mutex

func AppendToFile(filename string, content string) error {
	mutex.Lock()
	defer mutex.Unlock()

	file, err := os.OpenFile(filename, os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("file %s create failed. err: %s", filename, err)
		return err
	}
	defer file.Close()

	n, _ := file.Seek(0, io.SeekEnd)
	_, err = file.WriteAt([]byte(content), n)
	return err
}

func Exists(path string) bool {
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}
