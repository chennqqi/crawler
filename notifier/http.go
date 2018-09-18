package notifier

import (
	"net/http"

	"log"

	"io/ioutil"

	"sync"

	"time"

	"github.com/champkeh/crawler/types"
	"github.com/gorilla/websocket"
)

type HttpPrintNotifier struct {
	printChan chan types.NotifyData
	running   bool
	sync.Mutex
	RateLimiter types.RateLimiter
}

func (o *HttpPrintNotifier) Print(data types.NotifyData) {
	for o.printChan == nil {
		time.Sleep(100 * time.Millisecond)
	}

	go func() {
		o.printChan <- data
	}()
}

var upgrader = websocket.Upgrader{}

func (o *HttpPrintNotifier) Run() {
	o.Lock()

	if o.running {
		o.Unlock()
		return
	}
	o.running = true

	if o.printChan == nil {
		o.printChan = make(chan types.NotifyData, 100)
	}
	o.Unlock()

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

	ticker := time.Tick(1 * time.Second)
	for {
		select {
		case notify := <-o.printChan:
			// send to client
			conn.WriteJSON(notify)
		default:
		}

		select {
		case <-ticker:
			data := struct {
				Type    string
				Elapsed time.Duration
				QPS     float64
			}{
				Type:    "v2", //todo: fix this to list or detail
				Elapsed: time.Since(types.T1),
				QPS:     o.RateLimiter.QPS(),
			}
			conn.WriteJSON(data)
		default:
		}
	}
}
