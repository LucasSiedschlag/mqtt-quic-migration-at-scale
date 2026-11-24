package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const pubackTimeout = 10 * time.Second // a message without a PUBACK after this time is a failure

type publisherSettings struct {
	brokerHost     string
	transport      string // "tls" or "quic"
	publishers     int
	connectRate    int // connection attempts per second, for all publishers together
	messageRate    int // messages per second, for each publisher
	payloadSize    int // bytes
	topicPrefix    string
	steadySeconds  int    // seconds of messages that must arrive
	paddingSeconds int    // extra seconds of messages, which can be lost when the publishers stop
	firstAddress   net.IP // source address of publisher 0; publisher i uses firstAddress + i; nil = the kernel selects
	timeLimit      time.Duration
	outputDir      string

	// address change test (address_change.go)
	reconnect bool            // Paho reconnects after a lost connection
	moveTimes []time.Duration // after publishing starts, each publisher moves to a new source address
}

func parsePublisherSettings(args []string) (publisherSettings, error) {
	var settings publisherSettings
	var firstAddress, moveTimes string
	flags := flag.NewFlagSet("pub", flag.ExitOnError)
	flags.StringVar(&settings.brokerHost, "h", "mqtt-broker", "broker host")
	flags.StringVar(&settings.transport, "transport", "quic", "tls or quic")
	flags.IntVar(&settings.publishers, "c", 1, "number of publishers")
	flags.IntVar(&settings.connectRate, "R", 400, "connection attempts per second, for all publishers together")
	flags.IntVar(&settings.messageRate, "rate", 1, "messages per second, for each publisher")
	flags.IntVar(&settings.payloadSize, "s", 256, "payload size in bytes")
	flags.StringVar(&settings.topicPrefix, "prefix", "bench", "publisher i publishes on <prefix>/<i>")
	flags.IntVar(&settings.steadySeconds, "steady", 30, "seconds of messages that must arrive")
	flags.IntVar(&settings.paddingSeconds, "pad", 3, "extra seconds of messages, which can be lost when the publishers stop")
	flags.StringVar(&firstAddress, "bind", "", "source address of publisher 0; publisher i uses this address + i (empty = the kernel selects)")
	flags.DurationVar(&settings.timeLimit, "limit", 120*time.Second, "the run stops after this time and exits with status 1")
	flags.StringVar(&settings.outputDir, "out", ".", "directory for pub.csv and pub.txt")
	flags.BoolVar(&settings.reconnect, "reconnect", false, "reconnect after a lost connection; writes reconnect.csv")
	flags.StringVar(&moveTimes, "move", "", "seconds after publishing starts, separated by commas, at which each publisher "+
		"moves to a new source address; needs -bind, sets -reconnect, writes move.csv")
	flags.Parse(args)

	if settings.connectRate < 1 || settings.messageRate < 1 {
		return settings, errors.New("-R and -rate must be at least 1")
	}
	if firstAddress != "" {
		settings.firstAddress = net.ParseIP(firstAddress).To4()
		if settings.firstAddress == nil {
			return settings, fmt.Errorf("-bind %q is not an IPv4 address", firstAddress)
		}
	}

	var err error
	if settings.moveTimes, err = parseMoveTimes(moveTimes); err != nil {
		return settings, err
	}
	if len(settings.moveTimes) > 0 {
		if settings.firstAddress == nil {
			return settings, errors.New("-move needs -bind")
		}
		if settings.publishers > addressesPerMove {
			return settings, fmt.Errorf("-move allows at most %d publishers", addressesPerMove)
		}
		settings.reconnect = true // a TLS client must reconnect from its new address
	}
	return settings, nil
}

// addressOf returns the source address of publisher index: first + index.
func addressOf(first net.IP, index int) net.IP {
	address := make(net.IP, 4)
	binary.BigEndian.PutUint32(address, binary.BigEndian.Uint32(first.To4())+uint32(index))
	return address
}

// publisherRun contains the data that all publishers of one run share.
type publisherRun struct {
	settings  publisherSettings
	startedAt time.Time

	connected     atomic.Int64
	connectFailed atomic.Int64
	acked         atomic.Int64
	publishFailed atomic.Int64

	rowsLock       sync.Mutex
	connectionRows []string // one row of pub.csv for each connected publisher
	moveRows       []string // one row of move.csv for each move of each publisher
	reconnectRows  []string // one row of reconnect.csv for each lost connection

	startPublishing chan struct{}  // closed when all connection attempts have started
	connectionPhase sync.WaitGroup // done when each publisher is connected or failed
	allPublishers   sync.WaitGroup // done when all publishers are finished
}

// runPublishers runs the publisher side of one run: first the connection storm, then the messages.
func runPublishers(args []string) error {
	settings, err := parsePublisherSettings(args)
	if err != nil {
		return err
	}
	run := &publisherRun{settings: settings, startedAt: time.Now(), startPublishing: make(chan struct{})}
	time.AfterFunc(settings.timeLimit, run.cut)
	go run.printProgress()

	// connection storm: one new publisher each 1/connectRate seconds
	pacer := time.NewTicker(time.Second / time.Duration(settings.connectRate))
	for index := range settings.publishers {
		<-pacer.C
		run.connectionPhase.Add(1)
		run.allPublishers.Add(1)
		go run.publisher(index)
	}
	pacer.Stop()
	close(run.startPublishing)

	// write pub.csv when the connection phase ends, so that a run that stops later keeps it
	run.connectionPhase.Wait()
	if err := run.writeConnections(); err != nil {
		return err
	}
	run.allPublishers.Wait()
	if err := run.writeAddressChangeFiles(); err != nil {
		return err
	}
	return run.writeSummary(false)
}

// publisher connects publisher number index. It waits until all connection attempts have started, then publishes.
func (run *publisherRun) publisher(index int) {
	defer run.allPublishers.Done()
	clientID := fmt.Sprintf("gen_pub_%d", index)
	var lostAt atomic.Int64 // time of a lost connection (Unix ms) until the reconnection; 0 = connected
	client, dialer, err := run.connect(index, clientID, &lostAt)
	if err != nil {
		run.connectFailed.Add(1)
		run.connectionPhase.Done()
		return
	}
	run.connected.Add(1)
	run.connectionPhase.Done()

	<-run.startPublishing
	publishStart := time.Now()
	var moves sync.WaitGroup
	for number, after := range run.settings.moveTimes {
		moves.Go(func() { run.moveAt(dialer, clientID, index, number+1, publishStart.Add(after)) })
	}
	run.sendMessages(client, fmt.Sprintf("%s/%d", run.settings.topicPrefix, index), publishStart)
	moves.Wait()
	if lost := lostAt.Load(); lost != 0 {
		run.recordReconnection(clientID, lost, 0) // the publisher did not reconnect
	}
	client.Disconnect(disconnectWaitMsec)
}

func (run *publisherRun) connect(index int, clientID string, lostAt *atomic.Int64) (mqtt.Client, *brokerDialer, error) {
	var sourceAddress net.IP
	if run.settings.firstAddress != nil {
		sourceAddress = addressOf(run.settings.firstAddress, index)
	}
	dialer, err := newBrokerDialer(run.settings.transport, run.settings.brokerHost, sourceAddress)
	if err != nil {
		return nil, nil, err
	}
	dialer.movable = len(run.settings.moveTimes) > 0
	var configure []func(*mqtt.ClientOptions)
	if run.settings.reconnect {
		configure = append(configure, run.reconnectOptions(clientID, lostAt))
	}
	client := newMQTTClient(dialer, clientID, nil, configure...)

	connectStart := time.Now()
	token := client.Connect()
	if !token.WaitTimeout(connectTimeout) {
		return nil, nil, errors.New("connect timed out")
	}
	if token.Error() != nil {
		return nil, nil, token.Error()
	}
	run.recordConnection(clientID, dialer.handshakeTime(), time.Since(connectStart))
	return client, dialer, nil
}

// sendMessages sends message m at start + m/messageRate. It does not wait for the PUBACK of the
// previous message (QoS 1 messages in flight). Thus, the offered rate does not change with the round
// trip time. A late publisher sends its next message immediately. A second goroutine counts the PUBACKs.
func (run *publisherRun) sendMessages(client mqtt.Client, topic string, start time.Time) {
	total := (run.settings.steadySeconds + run.settings.paddingSeconds) * run.settings.messageRate
	interval := time.Second / time.Duration(run.settings.messageRate)

	waitingForPuback := make(chan mqtt.Token, total)
	allCounted := make(chan struct{})
	go func() {
		for token := range waitingForPuback {
			if token.WaitTimeout(pubackTimeout) && token.Error() == nil {
				run.acked.Add(1)
			} else {
				run.publishFailed.Add(1)
			}
		}
		close(allCounted)
	}()

	for message := range total {
		time.Sleep(time.Until(start.Add(time.Duration(message) * interval)))
		waitingForPuback <- client.Publish(topic, qosAtLeastOnce, false, newPayload(run.settings.payloadSize, time.Now()))
	}
	close(waitingForPuback)
	<-allCounted
}

// cut stops a run that is longer than the time limit. This occurs when the broker cannot process the load.
func (run *publisherRun) cut() {
	if err := run.writeAddressChangeFiles(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
	}
	if err := run.writeSummary(true); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
	}
	fmt.Fprintf(os.Stderr, "cut after %s\n", run.settings.timeLimit)
	os.Exit(1)
}

func (run *publisherRun) printProgress() {
	for range time.Tick(time.Second) {
		fmt.Printf("%3.0fs connected %d, failed %d, acked %d\n",
			time.Since(run.startedAt).Seconds(), run.connected.Load(), run.connectFailed.Load(), run.acked.Load())
	}
}

func (run *publisherRun) recordConnection(clientID string, handshake, connect time.Duration) {
	run.rowsLock.Lock()
	defer run.rowsLock.Unlock()
	run.connectionRows = append(run.connectionRows,
		fmt.Sprintf("%s,%d,%d,%d\n", clientID, time.Now().UnixMilli(), handshake.Milliseconds(), connect.Milliseconds()))
}

// writeConnections writes pub.csv: one row for each connected publisher, times in ms.
// Handshake is the TLS or QUIC handshake. Connect is the time from the start of the connection to the MQTT CONNACK.
func (run *publisherRun) writeConnections() error {
	run.rowsLock.Lock()
	defer run.rowsLock.Unlock()
	content := "ClientId,ConnectedAt,Handshake,Connect\n" + strings.Join(run.connectionRows, "")
	return run.writeOutput("pub.csv", content)
}

func (run *publisherRun) writeOutput(fileName, content string) error {
	return os.WriteFile(filepath.Join(run.settings.outputDir, fileName), []byte(content), 0o644)
}

// writeSummary writes pub.txt: the settings of the run and the final counters, one name=value on each line.
func (run *publisherRun) writeSummary(cut bool) error {
	settings := run.settings
	firstAddress := "" // empty when the kernel selects the source addresses
	if settings.firstAddress != nil {
		firstAddress = settings.firstAddress.String()
	}
	lines := []string{
		fmt.Sprintf("transport=%s", settings.transport),
		fmt.Sprintf("publishers=%d", settings.publishers),
		fmt.Sprintf("connect_rate=%d", settings.connectRate),
		fmt.Sprintf("message_rate=%d", settings.messageRate),
		fmt.Sprintf("payload_size=%d", settings.payloadSize),
		fmt.Sprintf("topic_prefix=%s", settings.topicPrefix),
		fmt.Sprintf("steady_seconds=%d", settings.steadySeconds),
		fmt.Sprintf("padding_seconds=%d", settings.paddingSeconds),
		fmt.Sprintf("first_address=%s", firstAddress),
		fmt.Sprintf("time_limit=%s", settings.timeLimit),
		fmt.Sprintf("reconnect=%t", settings.reconnect),
		fmt.Sprintf("move_at=%s", formatMoveTimes(settings.moveTimes)),
		fmt.Sprintf("started_at=%s", run.startedAt.UTC().Format(time.RFC3339)),
		fmt.Sprintf("seconds=%.1f", time.Since(run.startedAt).Seconds()),
		fmt.Sprintf("connected=%d", run.connected.Load()),
		fmt.Sprintf("connect_failed=%d", run.connectFailed.Load()),
		fmt.Sprintf("acked=%d", run.acked.Load()),
		fmt.Sprintf("publish_failed=%d", run.publishFailed.Load()),
		fmt.Sprintf("cut=%t", cut),
	}
	return run.writeOutput("pub.txt", strings.Join(lines, "\n")+"\n")
}
