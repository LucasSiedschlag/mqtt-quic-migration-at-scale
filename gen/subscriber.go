package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// quietSecondsBeforeExit: after SIGTERM, the subscriber continues to receive until no message arrives
// for this number of seconds. The broker can still deliver messages after the publishers stop.
const quietSecondsBeforeExit = 2

// runSubscriber subscribes to all publisher topics and writes the latency of each message to sub.csv.
func runSubscriber(args []string) error {
	flags := flag.NewFlagSet("sub", flag.ExitOnError)
	brokerHost := flags.String("h", "mqtt-broker", "broker host")
	transport := flags.String("transport", "quic", "tls or quic")
	topic := flags.String("t", "bench/#", "topic filter")
	outputDir := flags.String("out", ".", "directory for sub.csv")
	flags.Parse(args)

	latencies, err := createLatencyFile(filepath.Join(*outputDir, "sub.csv"))
	if err != nil {
		return err
	}
	dialer, err := newBrokerDialer(*transport, *brokerHost, nil)
	if err != nil {
		return err
	}
	client := newMQTTClient(dialer, "gen_sub", latencies.record)
	if token := client.Connect(); !token.WaitTimeout(connectTimeout) || token.Error() != nil {
		return fmt.Errorf("connect: %v", token.Error())
	}
	if token := client.Subscribe(*topic, qosAtLeastOnce, nil); !token.WaitTimeout(connectTimeout) || token.Error() != nil {
		return fmt.Errorf("subscribe: %v", token.Error())
	}
	if err := latencies.markSubscribed(); err != nil {
		return err
	}
	waitForStopAndQuiet(latencies)
	return latencies.close()
}

// latencyFile is sub.csv: one row for each received message, with the receive time and the latency in ms.
// The file has a temporary name until the subscription is active. Then it gets its final name,
// which the compose healthcheck waits for.
type latencyFile struct {
	path     string
	file     *os.File
	lock     sync.Mutex
	writer   *bufio.Writer
	received atomic.Int64
}

func createLatencyFile(path string) (*latencyFile, error) {
	os.Remove(path) // from an earlier run
	file, err := os.Create(path + ".part")
	if err != nil {
		return nil, err
	}
	latencies := &latencyFile{path: path, file: file, writer: bufio.NewWriter(file)}
	latencies.writer.WriteString("ReceivedAt,Latency\n")
	return latencies, nil
}

// record is the Paho message handler.
func (latencies *latencyFile) record(_ mqtt.Client, message mqtt.Message) {
	latencies.received.Add(1)
	receivedAt := time.Now().UnixMilli()
	sentAt, ok := sentAtMillis(message.Payload())
	if !ok {
		return
	}
	latencies.lock.Lock()
	defer latencies.lock.Unlock()
	fmt.Fprintf(latencies.writer, "%d,%d\n", receivedAt, receivedAt-sentAt)
}

func (latencies *latencyFile) markSubscribed() error {
	return os.Rename(latencies.path+".part", latencies.path)
}

func (latencies *latencyFile) flush() {
	latencies.lock.Lock()
	defer latencies.lock.Unlock()
	latencies.writer.Flush()
}

func (latencies *latencyFile) close() error {
	latencies.flush()
	return latencies.file.Close()
}

// waitForStopAndQuiet writes the file to disk each second. It returns after a stop request (SIGTERM or Ctrl-C)
// and quietSecondsBeforeExit seconds without a message.
func waitForStopAndQuiet(latencies *latencyFile) {
	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, syscall.SIGTERM, os.Interrupt)
	stopRequested := false
	quietSeconds := 0
	var previousCount int64

	for range time.Tick(time.Second) {
		latencies.flush()
		count := latencies.received.Load()
		fmt.Printf("received %d\n", count)

		if count == previousCount {
			quietSeconds++
		} else {
			quietSeconds = 0
		}
		previousCount = count

		select {
		case <-stopSignal:
			stopRequested = true
		default:
		}
		if stopRequested && quietSeconds >= quietSecondsBeforeExit {
			return
		}
	}
}
