package types

type RequestScheduler interface {
	Submit(Request)
	ConfigureRequestChan(chan Request)
}

type FlightScheduler interface {
	Submit(FlightInfo)
	ConfigureFlightChan(chan FlightInfo)
}
