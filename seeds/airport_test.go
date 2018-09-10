package seeds

import (
	"testing"
)

func TestPullAirportList(t *testing.T) {
	chanAirports, err := PullAirportList()
	if err != nil {
		t.Errorf("open sql connection error: %v", err)
	}

	count := 0
	for _ = range chanAirports {
		//fmt.Printf("airport: %s -> %s\n", airport.DepCode, airport.ArrCode)
		count++
	}
	expected := 49502
	if count != expected {
		t.Errorf("airport count %d; expected %d", count, expected)
	}

}
