package utils

import (
	"time"
)

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
		return parse.Add(24*time.Hour).Format("2006-01-02") + " " + arrtime
	}
}
