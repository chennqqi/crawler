package notifer

import (
	"net/http"

	"log"

	"github.com/champkeh/crawler/types"
	"github.com/gorilla/websocket"
)

type HttpNotifier struct {
	outputChan chan types.NotifyData
}

func (o *HttpNotifier) ConfigureChan(ch chan types.NotifyData) {
	o.outputChan = ch
}

func (o *HttpNotifier) Notify(out types.NotifyData) {
	go func() {
		o.outputChan <- out
	}()
}

var upgrader = websocket.Upgrader{}

func (o *HttpNotifier) Run() {
	// start a web socket server
	http.HandleFunc("/", index)
	http.HandleFunc("/progress", o.Progress)
	log.Println("Start http server at localhost:8000...")
	http.ListenAndServe(":8000", nil)
}

var html = `<!doctype html>
<html>
<head>
<title>Crawler Monitor</title>
</head>
<body>
<h1>Crawler Monitor</h1>
<script>
	ws = new WebSocket("ws://localhost:8000/progress");
	ws.onopen = function(evt) {
		console.info("connected");
	};
	ws.onmessage = function(evt) {
		console.log(evt.data);
	};
	ws.onerror = function(evt) {
		console.error(evt);
	};
</script>
</body>
</html>`

func index(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(html))
}

func (o *HttpNotifier) Progress(w http.ResponseWriter, r *http.Request) {
	conn, _ := upgrader.Upgrade(w, r, nil)
	defer conn.Close()

	for {
		notify := <-o.outputChan
		// send to client
		conn.WriteJSON(notify)
	}
}
