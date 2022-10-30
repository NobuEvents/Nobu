package main

import (
	"fmt"
	"github.com/NobuEvents/Nobu/pkg/inputs/http_server"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var Config = struct {
	HttpServer http_server.Config
}{
	HttpServer: http_server.Config{
		ListenAddress: "localhost:8080",
	},
}

func main() {
	fmt.Println(`[N]obu
[O]perates
[B]asically
[U]nhindered 😎`)

	httpServer, err := http_server.NewServer(Config.HttpServer)
	if err != nil {
		log.Fatal(err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	select {
	case s := <-sigCh:
		log.Printf("Caught signal %s.", s)
		httpServer.Close()
	}
}
