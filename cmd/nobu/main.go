package main

import (
	"fmt"
	"github.com/NobuEvents/Nobu/pkg/inputs/http_server"
	"log"
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
	fmt.Printf("HTTP server listening at http://%s/", httpServer.Listener.Addr())
	select {}
}
