// gen is the MQTT load generator: gen pub (the publishers) and gen sub (the subscriber).
package main

import (
	"fmt"
	"os"
	"time"
)

const (
	connectTimeout     = 30 * time.Second
	qosAtLeastOnce     = 1
	disconnectWaitMsec = 250
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
