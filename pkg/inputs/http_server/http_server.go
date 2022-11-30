package http_server

import (
	"encoding/json"
	"fmt"
	"github.com/NobuEvents/Nobu/pkg/events"
	"github.com/NobuEvents/Nobu/pkg/inputs"
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

type Server struct {
	Config
	HttpServer *http.Server
	Listener   net.Listener

	output    chan events.Event
	serveOnce sync.Once
	closeOnce sync.Once
}

func NewServer(config Config) (*Server, error) {
	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("net.Listen() failed: %w", err)
	}

	output := make(chan events.Event, 64)
	return &Server{
		Config: config,
		HttpServer: &http.Server{
			Addr:    config.ListenAddress,
			Handler: http.HandlerFunc(httpHandler(output)),
		},
		Listener: listener,
		output:   output,
	}, nil
}

func (x *Server) Output() <-chan events.Event { return x.output }

func (x *Server) Server() {
	x.serveOnce.Do(func() {
		log.Printf("http server listening at http://%s/", x.Listener.Addr())
		if err := x.HttpServer.Serve(x.Listener); err != http.ErrServerClosed {
			log.Printf("http.Server.Serve() failed: %v", err)
		}
		log.Println("http server closed")
	})
}

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

func httpHandler(output chan<- events.Event) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")

		var request Request
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			log.Printf("json.Decode() failed: %v", err)
			if err := json.NewEncoder(w).Encode(inputs.Response{
				Status: inputs.StatusError,
				Error:  fmt.Sprintf("json.Decode() failed: %s", err),
			}); err != nil {
				log.Printf("w.Write() failed: %s", err)
			}
			return
		}

		acceptedEvents := make([]events.Event, 0, len(request.Events))
		for _, event := range request.Events {
			if err := event.Validate(); err != nil {
				log.Printf("event.Validate() failed: %v", err)
				continue
			}
			acceptedEvents = append(acceptedEvents, event.FillEmtpyFields())
		}

		for _, event := range acceptedEvents {
			output <- event
		}

		if err := json.NewEncoder(w).Encode(inputs.Response{
			Status: inputs.StatusOK,
			Count:  len(acceptedEvents),
		}); err != nil {
			log.Printf("w.Write() failed: %s", err)
		}
	}
}
