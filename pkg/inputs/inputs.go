package inputs

import "github.com/NobuEvents/Nobu/pkg/events"

type Input interface {
	Output() <-chan events.Event
	Close()
}
