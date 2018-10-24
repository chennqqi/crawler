package utils

import (
	"time"
)

// 返回格式为: 2018-10-01 10:00
func TimeToDatetime(date, deptime, arrtime string) string {
	if deptime == "" || arrtime == "" {
		return "1990-01-01 00:00"
	}

	if arrtime >= deptime {
		return date + " " + arrtime
	} else {
		parse, err := time.Parse("2006-01-02", date)
		if err != nil {
			panic(err)
		}
		return parse.AddDate(0, 0, 1).Format("2006-01-02") + " " + arrtime
	}
}
