package main

// The address change test (see DETAILS.md).

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/quic-go/quic-go"
)

const (
	addressesPerMove   = 16384 // at move k, publisher i uses firstAddress + i + k*16384: one /18 for each move
	addressBlockPrefix = 18
	publisherInterface = "pub0" // name given by compose.yaml
)

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

func formatMoveTimes(moveTimes []time.Duration) string {
	seconds := make([]string, len(moveTimes))
	for i, moveTime := range moveTimes {
		seconds[i] = strconv.Itoa(int(moveTime.Seconds()))
	}
	return strings.Join(seconds, ",")
}

// move breaks the connection, so Paho reconnects from the new address, or with migrate it migrates the QUIC connection.
func (dialer *brokerDialer) move(newAddress net.IP, migrate bool) error {
	dialer.lock.Lock()
	dialer.sourceAddress = newAddress
	tcpConnection, quicConnection, quicStream := dialer.tcpConnection, dialer.quicConnection, dialer.quicStream
	dialer.lock.Unlock()

	// A deadline in the past, not Close: Paho ignores the error of a closed connection.
	if dialer.transport == "tls" {
		if tcpConnection == nil {
			return errors.New("no connection to move")
		}
		return tcpConnection.SetDeadline(time.Unix(1, 0))
	}
	if !migrate {
		if quicStream == nil {
			return errors.New("no connection to move")
		}
		return quicStream.SetDeadline(time.Unix(1, 0))
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
	// Closing the previous path retires its connection ID.
	if dialer.movedPath != nil {
		dialer.movedPath.Close()
	}
	dialer.movedPath = path
	return nil
}

func (run *publisherRun) releaseMoves(publishStart time.Time) {
	for number, after := range run.settings.moveTimes {
		time.Sleep(time.Until(publishStart.Add(after)))
		if run.settings.moveDrop {
			previousBlock := addressOf(run.settings.firstAddress, number*addressesPerMove)
			if err := dropAddresses(previousBlock, number == 0); err != nil {
				fmt.Fprintf(os.Stderr, "move %d: %v\n", number+1, err)
			}
		}
		close(run.moveSignals[number])
	}
}

// dropAddresses uses tc, which drops after the socket without an error for the sender. An nftables drop
// returns EPERM, and quic-go closes the connection after a send error (see DETAILS.md).
func dropAddresses(block net.IP, firstBlock bool) error {
	prefix := fmt.Sprintf("%s/%d", block, addressBlockPrefix)
	commands := [][]string{
		{"filter", "add", "dev", publisherInterface, "egress", "protocol", "ip", "u32", "match", "ip", "src", prefix, "action", "drop"},
		{"filter", "add", "dev", publisherInterface, "ingress", "protocol", "ip", "u32", "match", "ip", "dst", prefix, "action", "drop"},
	}
	if firstBlock {
		commands = append([][]string{{"qdisc", "add", "dev", publisherInterface, "clsact"}}, commands...)
	}
	for _, command := range commands {
		if output, err := exec.Command("tc", command...).CombinedOutput(); err != nil {
			return fmt.Errorf("drop %s: tc %s: %v: %s", prefix, strings.Join(command, " "), err, output)
		}
	}
	fmt.Fprintf(os.Stderr, "dropped %s\n", prefix)
	return nil
}

func isBlockStart(address net.IP) bool {
	return binary.BigEndian.Uint32(address.To4())%addressesPerMove == 0
}

func (run *publisherRun) waitAndMove(dialer *brokerDialer, clientID string, index, move int) {
	<-run.moveSignals[move-1]
	startedAt := time.Now()
	err := dialer.move(addressOf(run.settings.firstAddress, index+move*addressesPerMove), run.settings.migrates())
	run.recordMove(clientID, move, startedAt, err)
}

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

func (run *publisherRun) recordMove(clientID string, move int, startedAt time.Time, err error) {
	doneMs, errorText := "", "" // DoneMs only for a QUIC migration; reconnect.csv has the times of a reconnection
	if err != nil {
		errorText = strings.ReplaceAll(err.Error(), ",", ";")
	} else if run.settings.migrates() {
		doneMs = strconv.FormatInt(time.Now().UnixMilli(), 10)
	}
	run.rowsLock.Lock()
	defer run.rowsLock.Unlock()
	run.moveRows = append(run.moveRows,
		fmt.Sprintf("%s,%d,%d,%s,%s\n", clientID, move, startedAt.UnixMilli(), doneMs, errorText))
}

func (run *publisherRun) recordReconnection(clientID string, lostMs, reconnectedMs int64) {
	run.rowsLock.Lock()
	defer run.rowsLock.Unlock()
	run.reconnectRows = append(run.reconnectRows, fmt.Sprintf("%s,%d,%d\n", clientID, lostMs, reconnectedMs))
}

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
