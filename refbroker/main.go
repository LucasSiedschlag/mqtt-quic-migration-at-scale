// refbroker is the MQTT broker of the benchmark: mochi-mqtt with two listeners that share one
// Go crypto/tls configuration, so only the transport differs between them.
//
//	TLS 1.3 over TCP   port 8883
//	MQTT over QUIC     UDP port 14567, ALPN "mqtt" (quic-go)
//
// Every client is allowed in; there is no authentication.
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
)

const (
	tlsAddress  = ":8883"
	quicAddress = ":14567"

	certificateFile = "/certs/server.pem" // made by certs.sh
	privateKeyFile  = "/certs/server.key"
)

// counterFiles are copied by saveCounters when the broker stops.
var counterFiles = []string{
	"/proc/net/snmp",          // kernel UDP counters, including receive errors
	"/proc/net/dev",           // bytes and packets per network interface
	"/sys/fs/cgroup/cpu.stat", // CPU time used by the container
}

func main() {
	if err := runBroker(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func runBroker() error {
	countersDir := flag.String("out", "", "directory for the counters saved when the broker stops (empty = none)")
	flag.Parse()

	tlsConfig, err := loadTLSConfig()
	if err != nil {
		return err
	}
	quicTLSConfig := tlsConfig.Clone()
	quicTLSConfig.NextProtos = []string{"mqtt"} // ALPN of MQTT over QUIC

	server := newMQTTServer()
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
	fmt.Fprintf(os.Stderr, "listening tls=%s quic=%s\n", tlsAddress, quicAddress) // the compose healthcheck waits for this line

	waitForStopSignal()
	if *countersDir != "" {
		saveCounters(*countersDir)
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

func newMQTTServer() *mochi.Server {
	capabilities := mochi.NewDefaultServerCapabilities()
	// The single subscriber receives every message of every publisher. With the default limits
	// its write queue and its in-flight window fill up at scale and the broker cuts it.
	capabilities.MaximumClientWritesPending = 1 << 20
	capabilities.MaximumInflight = 65535
	return mochi.New(&mochi.Options{
		Capabilities: capabilities,
		Logger:       slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
}

func waitForStopSignal() {
	stopSignal := make(chan os.Signal, 1)
	signal.Notify(stopSignal, syscall.SIGINT, syscall.SIGTERM)
	<-stopSignal
}

// saveCounters copies the counter files into dir as broker-<name>. A new container starts these counters
// at zero, so the copies hold the totals of one run.
func saveCounters(dir string) {
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
