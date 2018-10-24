package persist

import (
	"testing"

	"github.com/champkeh/crawler/fetcher"
	"github.com/champkeh/crawler/source/veryzhun"
	"github.com/champkeh/crawler/types"
	_ "github.com/denisenkom/go-mssqldb"
)

func TestVeryzhunUpdate(t *testing.T) {
	flight := types.FlightInfo{
		FlightNo:   "MF858",
		FlightDate: "2018-10-24",
	}
	request := veryzhun.DetailRequest(flight)
	result, err := fetcher.FetchRequest(request, nil)
	if err != nil {
		t.Errorf("fetch error: %s", err)
	}

	updateParseResultFromVeryzhun(result)
}
