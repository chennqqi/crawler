package types

type RequestScheduler interface {
	Submit(Request)
	ConfigureRequestChan(chan Request)
}

type FlightScheduler interface {
	Submit(FlightInfo)
	ConfigureFlightChan(chan FlightInfo)
}

type AirportScheduler interface {
	Submit(Airport)
	ConfigureAirportChan(chan Airport)
}
