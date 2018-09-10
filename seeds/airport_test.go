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
	expected := 49948
	if count != expected {
		t.Errorf("airport count %d; expected %d", count, expected)
	}
}

func TestAirportRequestFilter(t *testing.T) {
	//in := make(chan types.Airport)
	//for i := 0; i < 10; i++ {
	//	go func() {
	//		in <- types.Airport{
	//			DepCode: "SHA",
	//			ArrCode: "PEK",
	//		}
	//	}()
	//}
	//out := AirportRequestFilter(in)
	//counter := 0
	//for res := range out {
	//	if res.Dep != "SHA" {
	//		t.Errorf("dep code %s; expected %s", res.Dep, "SHA")
	//	}
	//	if res.Arr != "PEK" {
	//		t.Errorf("arr code %s; expected %s", res.Arr, "PEK")
	//	}
	//	counter++
	//}
	//if counter != 10 {
	//	t.Errorf("count %d; expected %d", counter, 10)
	//}
	t.SkipNow()
}
