package main

import (
	"encoding/binary"
	"time"
)

// The payload starts with its send time (Unix ms, 8 bytes, big endian): the subscriber calculates the latency.
const timestampSize = 8

func newPayload(size int, sentAt time.Time) []byte {
	payload := make([]byte, max(size, timestampSize))
	binary.BigEndian.PutUint64(payload, uint64(sentAt.UnixMilli()))
	return payload
}

func sentAtMillis(payload []byte) (int64, bool) {
	if len(payload) < timestampSize {
		return 0, false
	}
	return int64(binary.BigEndian.Uint64(payload)), true
}
