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

const pubackTimeout = 10 * time.Second

type publisherSettings struct {
	brokerHost     string
	transport      string
	publishers     int
	connectRate    int // all publishers together
	messageRate    int // for each publisher
	payloadSize    int // bytes
	topicPrefix    string
	steadySeconds  int
	paddingSeconds int    // messages that may be lost when the publishers stop
	firstAddress   net.IP // publisher i binds firstAddress + i; nil = the kernel selects
	timeLimit      time.Duration
	outputDir      string

	reconnect bool
	moveTimes []time.Duration
	moveMode  string // QUIC: "migrate" or "reconnect"; TLS always reconnects
	zeroRTT   bool
	moveDrop  bool
}

func (settings publisherSettings) migrates() bool {
	return settings.transport == "quic" && settings.moveMode == "migrate"
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
	flags.StringVar(&settings.moveMode, "move-mode", "migrate", "QUIC at a move: migrate (same connection, new path) "+
		"or reconnect (new connection); TLS always reconnects")
	flags.BoolVar(&settings.zeroRTT, "0rtt", false, "QUIC: a reconnection sends the MQTT CONNECT as early data (0-RTT)")
	flags.BoolVar(&settings.moveDrop, "move-drop", false, "at each move, first drop the previous addresses with tc filters on "+
		publisherInterface+" (needs NET_ADMIN)")
	flags.Parse(args)

	if settings.connectRate < 1 || settings.messageRate < 1 {
		return settings, errors.New("-R and -rate must be at least 1")
	}
	if settings.moveMode != "migrate" && settings.moveMode != "reconnect" {
		return settings, fmt.Errorf("-move-mode %q: use migrate or reconnect", settings.moveMode)
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
		if settings.moveDrop && !isBlockStart(settings.firstAddress) {
			return settings, fmt.Errorf("-move-drop needs -bind at the start of a /%d block", addressBlockPrefix)
		}
		settings.reconnect = true // after a move, TLS must reconnect
	}
	return settings, nil
}

func addressOf(first net.IP, index int) net.IP {
	address := make(net.IP, 4)
	binary.BigEndian.PutUint32(address, binary.BigEndian.Uint32(first.To4())+uint32(index))
	return address
}

type publisherRun struct {
	settings  publisherSettings
	startedAt time.Time

	connected     atomic.Int64
	connectFailed atomic.Int64
	acked         atomic.Int64
	publishFailed atomic.Int64

	rowsLock       sync.Mutex
	connectionRows []string
	moveRows       []string
	reconnectRows  []string

	startPublishing chan struct{}   // closed when all connection attempts have started
	moveSignals     []chan struct{} // closed at the time of each move
	connectionPhase sync.WaitGroup  // done when each publisher is connected or failed
	allPublishers   sync.WaitGroup
}

func runPublishers(args []string) error {
	settings, err := parsePublisherSettings(args)
	if err != nil {
		return err
	}
	run := &publisherRun{settings: settings, startedAt: time.Now(), startPublishing: make(chan struct{})}
	for range settings.moveTimes {
		run.moveSignals = append(run.moveSignals, make(chan struct{}))
	}
	time.AfterFunc(settings.timeLimit, run.cut)
	run.printAddresses()
	go run.printProgress()

	pacer := time.NewTicker(time.Second / time.Duration(settings.connectRate))
	for index := range settings.publishers {
		<-pacer.C
		run.connectionPhase.Add(1)
		run.allPublishers.Add(1)
		go run.publisher(index)
	}
	pacer.Stop()
	close(run.startPublishing)
	go run.releaseMoves(time.Now())

	// written now, so that a run cut later keeps it
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

func (run *publisherRun) publisher(index int) {
	defer run.allPublishers.Done()
	clientID := fmt.Sprintf("gen_pub_%d", index)
	var lostAt atomic.Int64 // Unix ms of a lost connection until the reconnection; 0 = connected
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
	for number := range run.settings.moveTimes {
		moves.Go(func() { run.waitAndMove(dialer, clientID, index, number+1) })
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
	dialer.movable = len(run.settings.moveTimes) > 0 && run.settings.migrates()
	dialer.zeroRTT = run.settings.zeroRTT
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

// sendMessages does not wait for the PUBACK of the previous message, so the offered rate does not depend on the RTT.
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

func (run *publisherRun) printAddresses() {
	first, last := run.settings.firstAddress, run.settings.publishers-1
	if first == nil {
		fmt.Println("publisher addresses: chosen by the kernel")
		return
	}
	fmt.Printf("publishers 0 to %d: %s to %s\n", last, first, addressOf(first, last))
	for move := 1; move <= len(run.settings.moveTimes); move++ {
		fmt.Printf("move %d: %s to %s\n", move, addressOf(first, move*addressesPerMove), addressOf(first, last+move*addressesPerMove))
	}
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

func (run *publisherRun) writeConnections() error {
	run.rowsLock.Lock()
	defer run.rowsLock.Unlock()
	content := "ClientId,ConnectedAt,Handshake,Connect\n" + strings.Join(run.connectionRows, "")
	return run.writeOutput("pub.csv", content)
}

func (run *publisherRun) writeOutput(fileName, content string) error {
	return os.WriteFile(filepath.Join(run.settings.outputDir, fileName), []byte(content), 0o644)
}

func (run *publisherRun) writeSummary(cut bool) error {
	settings := run.settings
	firstAddress := ""
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
		fmt.Sprintf("move_mode=%s", settings.moveMode),
		fmt.Sprintf("zero_rtt=%t", settings.zeroRTT),
		fmt.Sprintf("move_drop=%t", settings.moveDrop),
		fmt.Sprintf("started_at=%s", run.startedAt.Format(time.RFC3339)), // local time
		fmt.Sprintf("started_at_ms=%d", run.startedAt.UnixMilli()),       // same unit as the csv times
		fmt.Sprintf("seconds=%.1f", time.Since(run.startedAt).Seconds()),
		fmt.Sprintf("connected=%d", run.connected.Load()),
		fmt.Sprintf("connect_failed=%d", run.connectFailed.Load()),
		fmt.Sprintf("acked=%d", run.acked.Load()),
		fmt.Sprintf("publish_failed=%d", run.publishFailed.Load()),
		fmt.Sprintf("cut=%t", cut),
	}
	return run.writeOutput("pub.txt", strings.Join(lines, "\n")+"\n")
}
