package utils

import "github.com/champkeh/crawler/ocr"

func Code2Time(code string) string {
	// 查数据库
	s, err := ocr.CodeToTime(code)
	if err != nil {
		// 数据库命中
		return ""
	}

	return s
}
