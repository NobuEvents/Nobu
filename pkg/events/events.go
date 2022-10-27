package events

import "errors"

type Event struct {
	Timestamp int64  `json:"timestamp"`
	Type      string `json:"type"`
	Host      string `json:"host"`
	Offset    int64  `json:"offset"`
	Message   []byte `json:"message"`
}

func (e Event) Validate() error {
	if e.Timestamp == 0 {
		return errors.New("timestamp cant be empty")
	}
	if e.Type == "" {
		return errors.New("type cant be empty")
	}
	return nil
}
