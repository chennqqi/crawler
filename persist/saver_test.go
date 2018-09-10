package persist

import (
	"testing"

	"github.com/champkeh/crawler/types"
	"github.com/champkeh/crawler/umetrip/parser"
)

func TestSave(t *testing.T) {
	var result = types.ParseResult{
		Param: types.Param{
			Dep:  "PEK",
			Arr:  "SHA",
			Date: "2018-09-10",
		},
		Items: []interface{}{
			parser.FlightListData{
				FlightNo:      "HO1252",
				FlightCompany: "吉祥航空",
				DepTimePlan:   "06:50",
				DepTimeActual: "07:21",
				ArrTimePlan:   "09:10",
				ArrTimeActual: "08:58",
				State:         "到达",
				Airport:       "T3/T2",
			},
			parser.FlightListData{
				FlightNo:      "MU3924",
				FlightCompany: "东方航空",
				DepTimePlan:   "06:50",
				DepTimeActual: "07:21",
				ArrTimePlan:   "09:10",
				ArrTimeActual: "08:58",
				State:         "到达",
				Airport:       "T3/T2",
			},
		},
	}

	err := Save(result, nil)
	if err != nil {
		t.Errorf("save error: %v", err)
	}
}
