package main

import (
	"encoding/binary"
	"time"
)

// The payload carries the time it was sent, so the subscriber can compute the delivery latency.
// Layout: 8 bytes with the send time (Unix milliseconds, big endian), then zeros up to the payload size.
const timestampSize = 8

func newPayload(size int, sentAt time.Time) []byte {
	payload := make([]byte, max(size, timestampSize))
	binary.BigEndian.PutUint64(payload, uint64(sentAt.UnixMilli()))
	return payload
}

// sentAtMillis returns the send time written by newPayload, in Unix milliseconds.
func sentAtMillis(payload []byte) (int64, bool) {
	if len(payload) < timestampSize {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(payload)), true
}
