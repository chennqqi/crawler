package notifier

import (
	"net/http"

	"log"

	"io/ioutil"

	"github.com/champkeh/crawler/types"
	"github.com/gorilla/websocket"
)

type HttpPrintNotifier struct {
	printChan chan types.NotifyData
}

func (o *HttpPrintNotifier) ConfigureChan(channel chan types.NotifyData) {
	o.printChan = channel
}

func (o *HttpPrintNotifier) Print(data types.NotifyData) {
	go func() {
		o.printChan <- data
	}()
}

var upgrader = websocket.Upgrader{}

func (o *HttpPrintNotifier) Run() {

	// start a web socket server
	http.HandleFunc("/", index)
	http.HandleFunc("/progress", o.Progress)
	http.Handle("/static/", http.StripPrefix("/static/",
		http.FileServer(http.Dir("./notifier/assets/"))))

	log.Println("Start http server at localhost:8000...")
	http.ListenAndServe(":8000", nil)
}

func index(w http.ResponseWriter, r *http.Request) {
	content, err := ioutil.ReadFile("./notifier/assets/index.html")
	if err != nil {
		panic(err)
	}
	w.Write(content)
}

func (o *HttpPrintNotifier) Progress(w http.ResponseWriter, r *http.Request) {
	conn, _ := upgrader.Upgrade(w, r, nil)
	defer conn.Close()

	for {
		notify := <-o.printChan
		// send to client
		conn.WriteJSON(notify)
	}
}
