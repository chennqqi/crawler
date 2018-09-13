package proxy

import (
	"fmt"

	"github.com/champkeh/crawler/proxy/fetcher"
	"github.com/champkeh/crawler/proxy/types"
	"github.com/champkeh/crawler/proxy/verifier"
)

func Run() {
	out := make(chan types.ProxyIP, 100)
	for i := 0; i < 3; i++ {
		createFetchWorker(out)
	}

	verified := make(chan types.ProxyIP, 100)
	for i := 0; i < 100; i++ {
		createVerifyWorker(out, verified)
	}

	umetrip := make(chan types.ProxyIP, 100)
	for i := 0; i < 100; i++ {
		createUmetripVerifyWorker(verified, umetrip)
	}

	for {
		proxy := <-umetrip
		fmt.Println(proxy)
		//persist.Save(proxy)
	}
}

func createFetchWorker(out chan types.ProxyIP) {
	go func() {
		anonymousLevel := 0
		for {
			anonymousLevel++
			anonymousLevel %= 3

			ips, err := fetcher.FetchProxy(anonymousLevel)
			if err != nil {
				continue
			}

			for _, ip := range ips {
				out <- ip
			}
		}
	}()
}

func createVerifyWorker(in chan types.ProxyIP, out chan types.ProxyIP) {
	go func() {
		for {
			proxy := <-in
			err := verifier.VerifyProxy(proxy)
			if err != nil {
				continue
			}
			out <- proxy
		}
	}()
}

func createUmetripVerifyWorker(in chan types.ProxyIP, out chan types.ProxyIP) {
	go func() {
		for {
			proxy := <-in
			err := verifier.UmetripVerify(proxy)
			if err != nil {
				continue
			}
			out <- proxy
		}
	}()
}
