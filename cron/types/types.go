package types

type RealTimeTableModel struct {
	ID               int
	FlightNo         string
	Date             string
	DepCode          string
	ArrCode          string
	DepCity          string
	ArrCity          string
	FlightState      string
	DepPlanTime      string
	DepExpTime       string
	DepActualTime    string
	ArrPlanTime      string
	ArrExpTime       string
	ArrActualTime    string
	Mileage          string
	Duration         string
	Age              string
	PreFlightNo      string
	PreFlightState   string
	PreFlightDepCode string
	PreFlightArrCode string
	DepWeather       string
	ArrWeather       string
	DepFlow          string
	ArrFlow          string
	CheckinCounter   string
	BoardGate        string
	BaggageTurntable string
}
