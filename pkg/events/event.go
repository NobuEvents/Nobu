package events

import (
	"errors"
	"os"
	"sync/atomic"
	"time"
)

type Event struct {
	Type        string `json:"type"`
	TimestampNs int64  `json:"timestamp_ns"`
	Host        string `json:"host"`
	Offset      int64  `json:"offset"`
	Message     []byte `json:"message"`
}

var ErrTypeEmpty = errors.New("type cant be empty")
var ErrTimestampEmpty = errors.New("timestamp cant be empty")

var hostname, _ = os.Hostname()
var offset int64 = 0

func nextOffset() int64 { return atomic.AddInt64(&offset, 1) }

func (e Event) Validate() error {
	if e.Type == "" {
		return ErrTypeEmpty
	}
	if e.TimestampNs == 0 {
		return ErrTimestampEmpty
	}
	return nil
}

func (e Event) FillEmtpyFields() Event {
	if e.TimestampNs == 0 {
		e.TimestampNs = time.Now().UnixNano()
	}
	if e.Host == "" {
		e.Host = hostname
	}
	if e.Offset == 0 {
		e.Offset = nextOffset()
	}
	return e
}
