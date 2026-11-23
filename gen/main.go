// gen is the MQTT load generator of the benchmark.
//
//	gen pub [flags]   opens one MQTT connection per publisher and publishes on each of them
//	gen sub [flags]   subscribes to every publisher topic and records the latency of each message
//
// Run "gen pub -help" or "gen sub -help" for the flags.
package main

import (
	"fmt"
	"os"
	"time"
)

const (
	connectTimeout     = 30 * time.Second // for the TLS or QUIC handshake and for the MQTT CONNECT
	qosAtLeastOnce     = 1                // QoS 1: the broker acknowledges each message with a PUBACK
	disconnectWaitMsec = 250              // time Paho waits for pending work before it disconnects
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
