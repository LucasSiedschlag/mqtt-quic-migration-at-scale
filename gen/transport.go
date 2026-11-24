package main

// The two transports. They use the same MQTT client (Eclipse Paho, MQTT 3.1.1).
// Only the function that opens the network connection is different.

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

// brokerDialer opens one connection to the broker, over TCP + TLS 1.3 or over QUIC.
// It records the handshake time.
type brokerDialer struct {
	transport     string
	brokerAddress string // host:port
	tlsConfig     *tls.Config
	movable       bool         // QUIC: the connection must be able to migrate (address change test)
	handshake     atomic.Int64 // nanoseconds

	lock           sync.Mutex
	sourceAddress  net.IP     // local address to bind; nil = the kernel selects; move changes it
	tcpConnection  net.Conn   // TCP connection under the last TLS connection
	quicConnection *quic.Conn // last QUIC connection
	movedPath      *quic.Path // QUIC path in use after the last move
}

func newBrokerDialer(transport, brokerHost string, sourceAddress net.IP) (*brokerDialer, error) {
	port, known := brokerPorts[transport]
	if !known {
		return nil, fmt.Errorf("unknown transport %q, use tls or quic", transport)
	}
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // the benchmark does not measure certificate checks
		MinVersion:         tls.VersionTLS13,
		MaxVersion:         tls.VersionTLS13,
		KeyLogWriter:       tlsKeyLog(), // Wireshark uses this file to decrypt a capture
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

// tlsKeyLog is the file in SSLKEYLOGFILE, or nil. All connections share it.
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

func (dialer *brokerDialer) currentSourceAddress() net.IP {
	dialer.lock.Lock()
	defer dialer.lock.Unlock()
	return dialer.sourceAddress
}

// open gives Paho the network connection for MQTT.
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
	tcpDialer := net.Dialer{LocalAddr: &net.TCPAddr{IP: dialer.currentSourceAddress()}}
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
	dialer.lock.Lock()
	dialer.tcpConnection = tcpConnection
	dialer.lock.Unlock()
	return tlsConnection, nil
}

func (dialer *brokerDialer) openQUIC(ctx context.Context) (net.Conn, error) {
	start := time.Now()
	brokerUDPAddress, err := net.ResolveUDPAddr("udp4", dialer.brokerAddress)
	if err != nil {
		return nil, err
	}
	// shortcut: the UDP socket stays open after the connection closes. There is one socket for each publisher; the process frees them when it exits.
	udpSocket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: dialer.currentSourceAddress()})
	if err != nil {
		return nil, err
	}
	quicConfig := &quic.Config{
		HandshakeIdleTimeout: connectTimeout,
		MaxIdleTimeout:       120 * time.Second,
		KeepAlivePeriod:      15 * time.Second,
	}
	var quicConnection *quic.Conn
	if dialer.movable {
		// quic.Dial uses connection IDs of zero length, and a connection without an ID cannot migrate.
		transport := &quic.Transport{Conn: udpSocket, ConnectionIDLength: 4}
		quicConnection, err = transport.Dial(ctx, brokerUDPAddress, dialer.tlsConfig, quicConfig)
	} else {
		quicConnection, err = quic.Dial(ctx, udpSocket, brokerUDPAddress, dialer.tlsConfig, quicConfig)
	}
	if err != nil {
		udpSocket.Close()
		return nil, err
	}
	dialer.handshake.Store(int64(time.Since(start))) // Dial returns when the handshake is complete
	dialer.lock.Lock()
	dialer.quicConnection = quicConnection
	dialer.lock.Unlock()

	// the first bidirectional stream from the client carries the MQTT connection
	stream, err := quicConnection.OpenStreamSync(ctx)
	if err != nil {
		quicConnection.CloseWithError(0, "")
		return nil, err
	}
	return quicStreamConn{Stream: stream, connection: quicConnection}, nil
}

// quicStreamConn makes a QUIC stream look like the net.Conn that Paho expects.
// Read, Write and the deadlines come from the stream. The addresses come from the connection.
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

// newMQTTClient returns a Paho client that connects through dialer. The client does not reconnect
// and does not retry a connection. Thus, each failed connection counts. configure can change these options,
// for example to reconnect in the address change test.
func newMQTTClient(dialer *brokerDialer, clientID string, onMessage mqtt.MessageHandler, configure ...func(*mqtt.ClientOptions)) mqtt.Client {
	options := mqtt.NewClientOptions().
		AddBroker("tcp://" + dialer.brokerAddress). // only a label: dialer.open makes the connection
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
	for _, change := range configure {
		change(options)
	}
	return mqtt.NewClient(options)
}
