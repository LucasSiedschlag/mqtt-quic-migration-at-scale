package main

import (
	"net"
	"testing"
	"time"
)

func TestPayloadCarriesSendTime(t *testing.T) {
	sentAt := time.UnixMilli(1794397323123)
	payload := newPayload(256, sentAt)
	if len(payload) != 256 {
		t.Fatalf("payload has %d bytes, want 256", len(payload))
	}
	millis, ok := sentAtMillis(payload)
	if !ok || millis != 1794397323123 {
		t.Fatalf("send time %d (ok %v), want 1794397323123", millis, ok)
	}
}

func TestShortPayloadHasNoSendTime(t *testing.T) {
	if _, ok := sentAtMillis([]byte{1, 2}); ok {
		t.Fatal("a 2 byte payload was read as having a send time")
	}
}

func TestAddressOfCrossesByteBoundaries(t *testing.T) {
	address := addressOf(net.ParseIP("10.77.100.0"), 4999)
	if address.String() != "10.77.119.135" {
		t.Fatalf("publisher 4999 got %s, want 10.77.119.135", address)
	}
}
