package main

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"runtime"
	"sync"
	"time"

	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/quic-go/quic-go"
)

const quicListenerID = "quic"

// quicListener is a mochi-mqtt listener on quic-go. On each QUIC connection, the first
// bidirectional stream from the client carries the MQTT connection.
type quicListener struct {
	address   string
	tlsConfig *tls.Config

	listener  *quic.Listener
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
}

func (listener *quicListener) ID() string       { return quicListenerID }
func (listener *quicListener) Address() string  { return listener.address }
func (listener *quicListener) Protocol() string { return "quic" }

// Init opens the UDP socket and starts to listen for QUIC connections.
func (listener *quicListener) Init(*slog.Logger) error {
	listener.ctx, listener.cancel = context.WithCancel(context.Background())
	udpAddress, err := net.ResolveUDPAddr("udp4", listener.address)
	if err != nil {
		return err
	}
	udpSocket, err := net.ListenUDP("udp4", udpAddress)
	if err != nil {
		return err
	}
	transport := &quic.Transport{Conn: udpSocket}
	listener.listener, err = transport.Listen(listener.tlsConfig, &quic.Config{MaxIdleTimeout: 120 * time.Second})
	return err
}

// Serve accepts QUIC connections until the listener closes. It gives each MQTT stream to mochi-mqtt.
//
// When its accept queue (32 entries) is full, quic-go refuses (CONNECTION_REFUSED) connections
// with a completed handshake. During a connection storm, one accept loop cannot empty the queue
// fast enough. Thus, several loops run in parallel.
func (listener *quicListener) Serve(establish listeners.EstablishFn) {
	var acceptLoops sync.WaitGroup
	for range 4 * runtime.NumCPU() {
		acceptLoops.Go(func() {
			for {
				connection, err := listener.listener.Accept(listener.ctx)
				if err != nil {
					return // the listener is closed
				}
				go listener.serveConnection(connection, establish)
			}
		})
	}
	acceptLoops.Wait()
}

// serveConnection waits for the first stream from the client and runs the MQTT session on it.
func (listener *quicListener) serveConnection(connection *quic.Conn, establish listeners.EstablishFn) {
	stream, err := connection.AcceptStream(listener.ctx)
	if err != nil {
		connection.CloseWithError(0, "")
		return
	}
	establish(quicListenerID, quicStreamConn{Stream: stream, connection: connection})
}

func (listener *quicListener) Close(closeClients listeners.CloseFn) {
	listener.closeOnce.Do(func() {
		closeClients(quicListenerID)
		listener.cancel()
		listener.listener.Close()
	})
}

// quicStreamConn makes a QUIC stream look like the net.Conn that mochi-mqtt expects.
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
