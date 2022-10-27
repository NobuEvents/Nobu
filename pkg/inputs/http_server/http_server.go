package http_server

import (
	"encoding/json"
	"fmt"
	"github.com/NobuEvents/Nobu/pkg/events"
	"log"
	"net"
	"net/http"
	"sync"
)

type Config struct {
	ListenAddress string `json:"listen_address"`
}

type Request struct {
	Events []events.Event `json:"events"`
}

type Response struct {
	Status string `json:"status"`
	Count  int    `json:"count"`
	Error  string `json:"error,omitempty"`
}

type Server struct {
	Config
	HttpServer *http.Server
	Listener   net.Listener

	output    chan events.Event
	closeOnce sync.Once
}

func NewServer(config Config) (*Server, error) {
	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("net.Listen() failed: %w", err)
	}

	output := make(chan events.Event, 64)
	server := &Server{
		Config: config,
		HttpServer: &http.Server{
			Addr:    config.ListenAddress,
			Handler: http.HandlerFunc(httpHandler(output)),
		},
		Listener: listener,
		output:   output,
	}

	go func() {
		log.Println(server.HttpServer.Serve(server.Listener))
	}()
	return server, nil
}

func (x *Server) Output() <-chan events.Event { return x.output }

func (x *Server) Close() {
	x.closeOnce.Do(func() {
		if x.HttpServer != nil {
			if err := x.HttpServer.Close(); err != nil {
				log.Printf("http.HttpServer.Close() failed: %v", err)
			}
		}
		close(x.output)
	})
}

func httpHandler(input chan<- events.Event) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var request Request
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			err := json.NewEncoder(w).Encode(Response{
				Status: "error",
				Error:  fmt.Sprintf("json.Decode() failed: %s", err),
			})
			if err != nil {
				log.Printf("w.Write() failed: %s", err)
			}
		}

		for _, event := range request.Events {
			input <- event
		}

		err := json.NewEncoder(w).Encode(Response{
			Status: "ok",
			Count:  len(request.Events),
		})
		if err != nil {
			log.Printf("w.Write() failed: %s", err)
		}
	}
}
