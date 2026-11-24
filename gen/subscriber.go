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

// After SIGTERM, the broker can still be delivering: wait until no message arrives for this time.
const quietSecondsBeforeExit = 2

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
	fmt.Printf("subscribed to %s from %s\n", *topic, dialer.localIP())
	waitForStopAndQuiet(latencies)
	return latencies.close()
}

// latencyFile is sub.csv, named sub.csv.part until the subscription holds: the compose healthcheck waits for sub.csv.
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
	latencies.writer.WriteString("Topic,ReceivedAt,Latency\n")
	return latencies, nil
}

func (latencies *latencyFile) record(_ mqtt.Client, message mqtt.Message) {
	latencies.received.Add(1)
	receivedAt := time.Now().UnixMilli()
	sentAt, ok := sentAtMillis(message.Payload())
	if !ok {
		return
	}
	latencies.lock.Lock()
	defer latencies.lock.Unlock()
	fmt.Fprintf(latencies.writer, "%s,%d,%d\n", message.Topic(), receivedAt, receivedAt-sentAt)
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
