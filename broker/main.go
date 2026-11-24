// broker: mochi-mqtt with TLS 1.3 on TCP 8883 and MQTT over QUIC on UDP 14567, from one crypto/tls configuration.
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
)

const (
	tlsAddress  = ":8883"
	quicAddress = ":14567"

	certificateFile = "/certs/server.pem" // from certs.sh
	privateKeyFile  = "/certs/server.key"
)

var counterFiles = []string{
	"/proc/net/snmp",          // kernel UDP counters, with receive errors
	"/proc/net/dev",           // bytes and packets of each network interface
	"/sys/fs/cgroup/cpu.stat", // CPU time of the container
}

func main() {
	if err := runBroker(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func runBroker() error {
	countersDir := flag.String("out", "", "directory for the counters that the broker saves when it stops (empty = no counters)")
	maxPending := flag.Int("max-pending", 1<<20, "capacity of the outbound queue of each client, in messages")
	flag.Parse()

	tlsConfig, err := loadTLSConfig()
	if err != nil {
		return err
	}
	quicTLSConfig := tlsConfig.Clone()
	quicTLSConfig.NextProtos = []string{"mqtt"} // ALPN of MQTT over QUIC

	server := newMQTTServer(*maxPending)
	if err := server.AddHook(new(auth.AllowHook), nil); err != nil {
		return err
	}
	if err := server.AddListener(listeners.NewTCP(listeners.Config{ID: "tls", Address: tlsAddress, TLSConfig: tlsConfig})); err != nil {
		return err
	}
	if err := server.AddListener(&quicListener{address: quicAddress, tlsConfig: quicTLSConfig}); err != nil {
		return err
	}
	if err := server.Serve(); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "listening tls=%s quic=%s on %s\n", tlsAddress, quicAddress, interfaceAddresses())
	go logDroppedMessages(server)

	waitForStopSignal()
	if *countersDir != "" {
		saveCounters(*countersDir, server)
	}
	return server.Close()
}

func loadTLSConfig() (*tls.Config, error) {
	certificate, err := tls.LoadX509KeyPair(certificateFile, privateKeyFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
	}, nil
}

// maxPending: the default limits cut the subscriber at scale (see DETAILS.md for the values).
func newMQTTServer(maxPending int) *mochi.Server {
	capabilities := mochi.NewDefaultServerCapabilities()
	capabilities.MaximumClientWritesPending = int32(maxPending)
	capabilities.MaximumInflight = 65535
	return mochi.New(&mochi.Options{
		Capabilities: capabilities,
		Logger:       slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
}

func interfaceAddresses() string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "unknown addresses"
	}
	var addresses []string
	for _, networkInterface := range interfaces {
		if networkInterface.Flags&net.FlagLoopback != 0 {
			continue
		}
		interfaceAddresses, _ := networkInterface.Addrs()
		for _, address := range interfaceAddresses {
			if ipNetwork, ok := address.(*net.IPNet); ok && ipNetwork.IP.To4() != nil {
				addresses = append(addresses, fmt.Sprintf("%s (%s)", ipNetwork.IP, networkInterface.Name))
			}
		}
	}
	return strings.Join(addresses, ", ")
}

func waitForStopSignal() {
	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, syscall.SIGINT, syscall.SIGTERM)
	<-stopSignal
}

// logDroppedMessages: mochi-mqtt drops a message when the outbound queue of a client is full.
func logDroppedMessages(server *mochi.Server) {
	var logged int64
	for range time.Tick(time.Second) {
		if dropped := atomic.LoadInt64(&server.Info.MessagesDropped); dropped != logged {
			fmt.Fprintf(os.Stderr, "messages dropped %d\n", dropped)
			logged = dropped
		}
	}
}

// saveCounters: a new container starts these counters at zero, so the copies are the totals of one run.
func saveCounters(dir string, server *mochi.Server) {
	dropped := fmt.Sprintf("%d\n", atomic.LoadInt64(&server.Info.MessagesDropped))
	if err := os.WriteFile(filepath.Join(dir, "broker-messages-dropped"), []byte(dropped), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "saving counters:", err)
	}
	for _, source := range counterFiles {
		content, err := os.ReadFile(source)
		if err == nil {
			err = os.WriteFile(filepath.Join(dir, "broker-"+filepath.Base(source)), content, 0o644)
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "saving counters:", err)
		}
	}
}
