package main

// The address change test. During the steady phase, each publisher moves to a new source address,
// as a device that changes network. QUIC migrates the open connection (RFC 9000, section 9).
// TCP cannot change its address, so the client reconnects from the new address.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/quic-go/quic-go"
)

// At move k, publisher i moves to firstAddress + i + k*addressesPerMove.
const addressesPerMove = 16384

// parseMoveTimes reads the -move flag: seconds after publishing starts, separated by commas.
func parseMoveTimes(list string) ([]time.Duration, error) {
	var moveTimes []time.Duration
	for _, field := range strings.Split(list, ",") {
		if field == "" {
			continue
		}
		seconds, err := strconv.Atoi(field)
		if err != nil || seconds < 1 {
			return nil, fmt.Errorf("-move %q: use whole seconds above 0, separated by commas", list)
		}
		moveTimes = append(moveTimes, time.Duration(seconds)*time.Second)
	}
	return moveTimes, nil
}

// formatMoveTimes writes move times as -move reads them, for example "10,20".
func formatMoveTimes(moveTimes []time.Duration) string {
	seconds := make([]string, len(moveTimes))
	for i, moveTime := range moveTimes {
		seconds[i] = strconv.Itoa(int(moveTime.Seconds()))
	}
	return strings.Join(seconds, ",")
}

// move gives the connection of the dialer a new source address.
// TLS: move breaks the TCP connection. Paho then reconnects from the new address.
// QUIC: move migrates the open connection: a new UDP socket, a path validation
// (PATH_CHALLENGE and PATH_RESPONSE), then the switch to the new path.
func (dialer *brokerDialer) move(newAddress net.IP) error {
	dialer.lock.Lock()
	dialer.sourceAddress = newAddress
	tcpConnection, quicConnection := dialer.tcpConnection, dialer.quicConnection
	dialer.lock.Unlock()

	if dialer.transport == "tls" {
		if tcpConnection == nil {
			return errors.New("no connection to move")
		}
		// A deadline in the past makes each read and write fail. Close does not work here:
		// Paho ignores the "use of closed network connection" error, because it expects it after its own Close.
		return tcpConnection.SetDeadline(time.Unix(1, 0))
	}

	if quicConnection == nil {
		return errors.New("no connection to move")
	}
	udpSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: newAddress})
	if err != nil {
		return err
	}
	path, err := quicConnection.AddPath(&quic.Transport{Conn: udpSocket})
	if err != nil {
		udpSocket.Close()
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	if err := path.Probe(ctx); err != nil {
		return err
	}
	if err := path.Switch(); err != nil {
		return err
	}
	// Close the path of the previous move. This retires its connection ID, so the broker can issue a new one.
	if dialer.movedPath != nil {
		dialer.movedPath.Close()
	}
	dialer.movedPath = path
	return nil
}

// reconnectOptions makes Paho reconnect after a lost connection (default backoff).
// lostAt holds the time of a loss (Unix ms) until the reconnection; 0 means connected.
func (run *publisherRun) reconnectOptions(clientID string, lostAt *atomic.Int64) func(*mqtt.ClientOptions) {
	return func(options *mqtt.ClientOptions) {
		options.SetAutoReconnect(true)
		options.SetConnectionLostHandler(func(mqtt.Client, error) {
			lostAt.CompareAndSwap(0, time.Now().UnixMilli())
		})
		options.SetOnConnectHandler(func(mqtt.Client) {
			if lost := lostAt.Swap(0); lost != 0 {
				run.recordReconnection(clientID, lost, time.Now().UnixMilli())
			}
		})
	}
}

// moveAt waits until the time of move number move, then moves the publisher and records the result.
func (run *publisherRun) moveAt(dialer *brokerDialer, clientID string, index, move int, at time.Time) {
	time.Sleep(time.Until(at))
	startedAt := time.Now()
	err := dialer.move(addressOf(run.settings.firstAddress, index+move*addressesPerMove))
	run.recordMove(clientID, move, startedAt, err)
}

// recordMove adds a row to move.csv. DoneMs is the end of a QUIC migration. It stays empty
// for TLS, because the TLS reconnection is in reconnect.csv.
func (run *publisherRun) recordMove(clientID string, move int, startedAt time.Time, err error) {
	doneMs, errorText := "", ""
	if err != nil {
		errorText = strings.ReplaceAll(err.Error(), ",", ";")
	} else if run.settings.transport == "quic" {
		doneMs = strconv.FormatInt(time.Now().UnixMilli(), 10)
	}
	run.rowsLock.Lock()
	defer run.rowsLock.Unlock()
	run.moveRows = append(run.moveRows,
		fmt.Sprintf("%s,%d,%d,%s,%s\n", clientID, move, startedAt.UnixMilli(), doneMs, errorText))
}

// recordReconnection adds a row to reconnect.csv. reconnectedMs is 0 when the publisher did not reconnect.
func (run *publisherRun) recordReconnection(clientID string, lostMs, reconnectedMs int64) {
	run.rowsLock.Lock()
	defer run.rowsLock.Unlock()
	run.reconnectRows = append(run.reconnectRows, fmt.Sprintf("%s,%d,%d\n", clientID, lostMs, reconnectedMs))
}

// writeAddressChangeFiles writes move.csv and reconnect.csv, when the run uses them.
func (run *publisherRun) writeAddressChangeFiles() error {
	run.rowsLock.Lock()
	defer run.rowsLock.Unlock()
	if len(run.settings.moveTimes) > 0 {
		content := "ClientId,Move,StartMs,DoneMs,Error\n" + strings.Join(run.moveRows, "")
		if err := run.writeOutput("move.csv", content); err != nil {
			return err
		}
	}
	if run.settings.reconnect {
		content := "ClientId,LostMs,ReconnectedMs\n" + strings.Join(run.reconnectRows, "")
		return run.writeOutput("reconnect.csv", content)
	}
	return nil
}
