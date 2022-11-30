package inputs

import "github.com/NobuEvents/Nobu/pkg/events"

type Input interface {
	Output() <-chan events.Event
	Close()
}

type Response struct {
	Status string `json:"status"`
	Count  int    `json:"count"`
	Error  string `json:"error,omitempty"`
}

const StatusOK = "ok"
const StatusError = "error"
