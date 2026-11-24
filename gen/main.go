// gen is the MQTT load generator of the benchmark.
//
//	gen pub [flags]   opens one MQTT connection for each publisher and publishes on it
//	gen sub [flags]   subscribes to all publisher topics and records the latency of each message
//
// "gen pub -help" and "gen sub -help" show the flags.
package main

import (
	"fmt"
	"os"
	"time"
)

const (
	connectTimeout     = 30 * time.Second // limit for the TLS or QUIC handshake and for the MQTT CONNECT
	qosAtLeastOnce     = 1                // QoS 1: the broker sends a PUBACK for each message
	disconnectWaitMsec = 250              // Paho waits this time for pending work, then disconnects
)

func main() {
	if len(os.Args) < 2 {
		exitWithUsage()
	}
	var err error
	switch os.Args[1] {
	case "pub":
		err = runPublishers(os.Args[2:])
	case "sub":
		err = runSubscriber(os.Args[2:])
	default:
		exitWithUsage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func exitWithUsage() {
	fmt.Fprintln(os.Stderr, "usage: gen pub|sub [flags]")
	os.Exit(2)
}
