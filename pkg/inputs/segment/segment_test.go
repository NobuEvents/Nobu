package segment

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

func TestSegmentServer(t *testing.T) {
	server, err := NewServer(Config{ListenAddress: "localhost:0"})
	require.NoError(t, err)
	defer server.Close()
	go server.Server()

	segmentPayload := `{
		"version"   : 1,
		"type"      : "identify",
		"userId"    : "019mr8mf4r",
		"traits"    : {
			"email"            : "achilles@segment.com",
			"name"             : "Achilles",
			"subscriptionPlan" : "Premium",
			"friendCount"      : 29
		},
		"timestamp" : "2012-12-02T00:30:08.276Z"
	}`

	resp, err := http.Post(
		fmt.Sprintf("http://%s/", server.Listener.Addr()),
		"application/json",
		strings.NewReader(segmentPayload),
	)
	require.NoError(t, err)

	var body bytes.Buffer
	_, err = body.ReadFrom(resp.Body)
	assert.Equal(t, `{"status":"ok","count":1}`, strings.TrimSpace(body.String()), "http.Response.Body")

	require.Equal(t, 1, len(server.Output()), "len(server.Output())")
	assert.Equal(t, <-server.Output(), events.Event{
		TimestampNs: 1354408208276000000,
		Type:        "identify",
		Message:     []byte(segmentPayload),
		Offset:      0,
		Host:        "",
	})
}
