package main

// The two transports. The MQTT client (Eclipse Paho, MQTT 3.1.1) is the same for both;
// only the function that opens the network connection changes.

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/quic-go/quic-go"
)

var brokerPorts = map[string]string{"tls": "8883", "quic": "14567"}

// brokerDialer opens one connection to the broker, over TCP + TLS 1.3 or over QUIC,
// and remembers how long the handshake took.
type brokerDialer struct {
	transport     string
	brokerAddress string // host:port
	sourceAddress net.IP // local address to bind; nil = chosen by the kernel
	tlsConfig     *tls.Config
	handshake     atomic.Int64 // nanoseconds
}

func newBrokerDialer(transport, brokerHost string, sourceAddress net.IP) (*brokerDialer, error) {
	port, known := brokerPorts[transport]
	if !known {
		return nil, fmt.Errorf("unknown transport %q, use tls or quic", transport)
	}
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // certificate checks are not what the benchmark measures
		MinVersion:         tls.VersionTLS13,
		MaxVersion:         tls.VersionTLS13,
		KeyLogWriter:       tlsKeyLog(), // lets Wireshark decrypt a capture
	}
	if transport == "quic" {
		tlsConfig.NextProtos = []string{"mqtt"} // ALPN of MQTT over QUIC
	}
	return &brokerDialer{
		transport:     transport,
		brokerAddress: net.JoinHostPort(brokerHost, port),
		sourceAddress: sourceAddress,
		tlsConfig:     tlsConfig,
	}, nil
}

// tlsKeyLog is the file named by SSLKEYLOGFILE, opened once and shared by every connection, or nil.
var tlsKeyLog = sync.OnceValue(func() io.Writer {
	path := os.Getenv("SSLKEYLOGFILE")
	if path == "" {
		return nil
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		fmt.Fprintln(os.Stderr, "SSLKEYLOGFILE:", err)
		return nil
	}
	return file
})

func (dialer *brokerDialer) handshakeTime() time.Duration {
	return time.Duration(dialer.handshake.Load())
}

// open is called by Paho to get the connection it speaks MQTT over.
func (dialer *brokerDialer) open(*url.URL, mqtt.ClientOptions) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), connectTimeout)
	defer cancel()
	if dialer.transport == "tls" {
		return dialer.openTLS(ctx)
	}
	return dialer.openQUIC(ctx)
}

func (dialer *brokerDialer) openTLS(ctx context.Context) (net.Conn, error) {
	start := time.Now()
	tcpDialer := net.Dialer{LocalAddr: &net.TCPAddr{IP: dialer.sourceAddress}}
	tcpConnection, err := tcpDialer.DialContext(ctx, "tcp4", dialer.brokerAddress)
	if err != nil {
		return nil, err
	}
	tlsConnection := tls.Client(tcpConnection, dialer.tlsConfig)
	if err := tlsConnection.HandshakeContext(ctx); err != nil {
		tcpConnection.Close()
		return nil, err
	}
	dialer.handshake.Store(int64(time.Since(start)))
	return tlsConnection, nil
}

func (dialer *brokerDialer) openQUIC(ctx context.Context) (net.Conn, error) {
	start := time.Now()
	brokerUDPAddress, err := net.ResolveUDPAddr("udp4", dialer.brokerAddress)
	if err != nil {
		return nil, err
	}
	// shortcut: the UDP socket stays open after the connection closes (one per publisher, freed when the process exits)
	udpSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: dialer.sourceAddress})
	if err != nil {
		return nil, err
	}
	quicConnection, err := quic.Dial(ctx, udpSocket, brokerUDPAddress, dialer.tlsConfig, &quic.Config{
		HandshakeIdleTimeout: connectTimeout,
		MaxIdleTimeout:       120 * time.Second,
		KeepAlivePeriod:      15 * time.Second,
	})
	if err != nil {
		udpSocket.Close()
		return nil, err
	}
	dialer.handshake.Store(int64(time.Since(start))) // quic.Dial returns once the handshake is complete

	// the first bidirectional stream the client opens carries the MQTT connection
	stream, err := quicConnection.OpenStreamSync(ctx)
	if err != nil {
		quicConnection.CloseWithError(0, "")
		return nil, err
	}
	return quicStreamConn{Stream: stream, connection: quicConnection}, nil
}

// quicStreamConn presents a QUIC stream as the net.Conn Paho expects.
// Read, Write and the deadlines come from the stream; the addresses come from the connection.
type quicStreamConn struct {
	*quic.Stream
	connection *quic.Conn
}

func (stream quicStreamConn) LocalAddr() net.Addr  { return stream.connection.LocalAddr() }
func (stream quicStreamConn) RemoteAddr() net.Addr { return stream.connection.RemoteAddr() }

func (stream quicStreamConn) Close() error {
	stream.Stream.Close()
	return stream.connection.CloseWithError(0, "")
}

// newMQTTClient returns a Paho client that connects through dialer. It never reconnects and never
// retries a connection, so every failed connection counts.
func newMQTTClient(dialer *brokerDialer, clientID string, onMessage mqtt.MessageHandler) mqtt.Client {
	options := mqtt.NewClientOptions().
		AddBroker("tcp://" + dialer.brokerAddress). // only a label: the connection comes from dialer.open
		SetClientID(clientID).
		SetProtocolVersion(4). // MQTT 3.1.1
		SetCleanSession(true).
		SetKeepAlive(60 * time.Second).
		SetConnectTimeout(connectTimeout).
		SetWriteTimeout(connectTimeout).
		SetAutoReconnect(false).
		SetConnectRetry(false).
		SetOrderMatters(false).
		SetCustomOpenConnectionFn(dialer.open)
	if onMessage != nil {
		options.SetDefaultPublishHandler(onMessage)
	}
	return mqtt.NewClient(options)
}
