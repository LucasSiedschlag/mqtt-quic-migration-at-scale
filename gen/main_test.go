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

func TestParseMoveTimes(t *testing.T) {
	moveTimes, err := parseMoveTimes("10,20")
	if err != nil || len(moveTimes) != 2 || moveTimes[0] != 10*time.Second || moveTimes[1] != 20*time.Second {
		t.Fatalf("got %v (error %v), want [10s 20s]", moveTimes, err)
	}
	if formatMoveTimes(moveTimes) != "10,20" {
		t.Fatalf("formatMoveTimes gave %q, want 10,20", formatMoveTimes(moveTimes))
	}
}

func TestEmptyMoveTimesMeanNoMove(t *testing.T) {
	moveTimes, err := parseMoveTimes("")
	if err != nil || len(moveTimes) != 0 {
		t.Fatalf("got %v (error %v), want no move", moveTimes, err)
	}
}

func TestBadMoveTimesAreRefused(t *testing.T) {
	for _, list := range []string{"ten", "10,x", "0", "-5", "1.5"} {
		if _, err := parseMoveTimes(list); err == nil {
			t.Errorf("-move %q was accepted", list)
		}
	}
}

func TestMoveNeedsBind(t *testing.T) {
	if _, err := parsePublisherSettings([]string{"-move", "10"}); err == nil {
		t.Fatal("-move without -bind was accepted")
	}
	settings, err := parsePublisherSettings([]string{"-move", "10,20", "-bind", "10.88.0.0"})
	if err != nil || !settings.reconnect {
		t.Fatalf("-move with -bind: error %v, reconnect %v; want no error and reconnect", err, settings.reconnect)
	}
}
