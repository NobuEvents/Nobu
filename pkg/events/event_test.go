package events

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestEvent_FillEmtpyFields(t *testing.T) {
	before := time.Now()
	hostname = "tester"
	offset = 0
	event := Event{}.FillEmtpyFields()
	assert.LessOrEqual(t, before.UnixNano(), event.TimestampNs, "Event.TimestampNs")
	assert.GreaterOrEqual(t, time.Now().UnixNano(), event.TimestampNs, "Event.TimestampNs")
}

func TestEvent_Validate(t *testing.T) {
	t.Run("Missing Type", func(t *testing.T) {
		assert.Equal(t, Event{
			TimestampNs: 77,
			Host:        "tester",
			Offset:      3,
			Message:     []byte("Nobu is better than you!"),
		}.Validate(), ErrTypeEmpty)
	})
	t.Run("Missing Type", func(t *testing.T) {
		assert.Equal(t, Event{
			Type:    "NobuEvent",
			Host:    "tester",
			Offset:  3,
			Message: []byte("Nobu is better than you!"),
		}.Validate(), ErrTimestampEmpty)
	})
}
