package segment

import (
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
	"time"
)

func TestTime(t *testing.T) {
	segmentTime, err := time.Parse(time.RFC3339, "2012-12-02T00:30:08.276Z")
	if err != nil {

		log.Fatal(err)
	}

	assert.Equal(t, int64(1354408208276000000), segmentTime.UnixNano(), "Check the Unix Nano seconds to date conversion")
}
