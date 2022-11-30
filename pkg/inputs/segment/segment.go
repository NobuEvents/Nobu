package segment

import (
	"encoding/json"
	"fmt"
	"github.com/NobuEvents/Nobu/pkg/events"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

type Config struct {
	ListenAddress string `json:"listen_address"`
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

		m := make(map[string]interface{})
		defer r.Body.Close()

		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("io.ReadAll(r.Body) failed: %v", err)
			return
		}

		err = json.Unmarshal(body, &m)
		if err != nil {
			log.Printf("json.Unmarshal failed: %v", err)
			return
		}

		segmentType := fmt.Sprintf("%v", m["type"])
		segmentTimestamp, err := time.Parse(time.RFC3339, fmt.Sprintf("%v", m["timestamp"]))
		if err != nil {
			log.Printf("time.Parse failed: %v", err)
			return
		}
		event := &events.Event{Type: segmentType, TimestampNs: segmentTimestamp.UnixNano(), Message: body}
		if err := event.Validate(); err != nil {
			log.Printf("event.Validate() failed: %v", err)
		}
		output <- *event
	}
}
