package main

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

type brokerDialer struct {
	transport     string
	brokerAddress string
	tlsConfig     *tls.Config
	movable       bool // QUIC migration (address change test)
	zeroRTT       bool
	handshake     atomic.Int64 // nanoseconds

	lock           sync.Mutex
	sourceAddress  net.IP // nil = the kernel selects; move changes it
	tcpConnection  net.Conn
	quicConnection *quic.Conn
	quicStream     *quic.Stream
	movedPath      *quic.Path
}

func newBrokerDialer(transport, brokerHost string, sourceAddress net.IP) (*brokerDialer, error) {
	port, known := brokerPorts[transport]
	if !known {
		return nil, fmt.Errorf("unknown transport %q, use tls or quic", transport)
	}
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // certificate checks are not measured
		MinVersion:         tls.VersionTLS13,
		MaxVersion:         tls.VersionTLS13,
		KeyLogWriter:       tlsKeyLog(), // for Wireshark
		// a reconnection resumes the TLS 1.3 session (PSK)
		ClientSessionCache: tls.NewLRUClientSessionCache(4),
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

// localIP: a UDP socket without a source address reports 0.0.0.0, so then ask the kernel which address it uses.
func (dialer *brokerDialer) localIP() net.IP {
	dialer.lock.Lock()
	var local net.Addr
	if dialer.quicConnection != nil {
		local = dialer.quicConnection.LocalAddr()
	} else if dialer.tcpConnection != nil {
		local = dialer.tcpConnection.LocalAddr()
	}
	dialer.lock.Unlock()

	var ip net.IP
	if address, ok := local.(*net.UDPAddr); ok {
		ip = address.IP
	} else if address, ok := local.(*net.TCPAddr); ok {
		ip = address.IP
	}
	if ip == nil || ip.IsUnspecified() {
		probe, err := net.Dial("udp4", dialer.brokerAddress) // a UDP "connect" sends no packet
		if err != nil {
			return ip
		}
		defer probe.Close()
		ip = probe.LocalAddr().(*net.UDPAddr).IP
	}
	return ip
}

func (dialer *brokerDialer) currentSourceAddress() net.IP {
	dialer.lock.Lock()
	defer dialer.lock.Unlock()
	return dialer.sourceAddress
}

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
	// shortcut: the UDP socket stays open after the connection closes, one for each publisher, freed at exit.
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
	switch {
	case dialer.movable:
		// quic.Dial uses connection IDs of zero length, and a connection without an ID cannot migrate.
		transport := &quic.Transport{Conn: udpSocket, ConnectionIDLength: 4}
		quicConnection, err = transport.Dial(ctx, brokerUDPAddress, dialer.tlsConfig, quicConfig)
	case dialer.zeroRTT:
		// with a session ticket, the MQTT CONNECT goes as early data (0-RTT)
		quicConnection, err = quic.DialEarly(ctx, udpSocket, brokerUDPAddress, dialer.tlsConfig, quicConfig)
	default:
		quicConnection, err = quic.Dial(ctx, udpSocket, brokerUDPAddress, dialer.tlsConfig, quicConfig)
	}
	if err != nil {
		udpSocket.Close()
		return nil, err
	}
	if dialer.zeroRTT && !dialer.movable {
		go dialer.recordQUICHandshake(quicConnection, start)
	} else {
		dialer.handshake.Store(int64(time.Since(start))) // Dial returns when the handshake is complete
	}

	// the first bidirectional stream from the client carries the MQTT connection
	stream, err := quicConnection.OpenStreamSync(ctx)
	if err != nil {
		quicConnection.CloseWithError(0, "")
		return nil, err
	}
	dialer.lock.Lock()
	dialer.quicConnection, dialer.quicStream = quicConnection, stream
	dialer.lock.Unlock()
	return quicStreamConn{Stream: stream, connection: quicConnection}, nil
}

// recordQUICHandshake waits for the handshake, because DialEarly returns before it.
func (dialer *brokerDialer) recordQUICHandshake(connection *quic.Conn, start time.Time) {
	select {
	case <-connection.HandshakeComplete():
		dialer.handshake.Store(int64(time.Since(start)))
	case <-connection.Context().Done():
	}
}

// quicStreamConn is the net.Conn that Paho expects, made from a QUIC stream.
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

// newMQTTClient does not reconnect or retry, so each failed connection counts. configure can change this.
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
