package http_server

import (
	"bytes"
	"fmt"
	"github.com/NobuEvents/Nobu/pkg/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"strings"
	"testing"
)

func TestServer(t *testing.T) {
	server, err := NewServer(Config{ListenAddress: "localhost:0"})
	require.NoError(t, err)
	defer server.Close()
	go server.Server()

	resp, err := http.Post(
		fmt.Sprintf("http://%s/", server.Listener.Addr()),
		"application/json",
		strings.NewReader(`{"events":[{"type":"cheetos","timestamp":1}]}`),
	)
	require.NoError(t, err)

	var body bytes.Buffer
	_, err = body.ReadFrom(resp.Body)
	assert.Equal(t, `{"status":"ok","count":1}`, strings.TrimSpace(body.String()), "http.Response.Body")

	require.Equal(t, 1, len(server.Output()), "len(server.Output())")
	assert.Equal(t, <-server.Output(), events.Event{
		TimestampNs: 1,
		Type:        "cheetos",
	})
}
