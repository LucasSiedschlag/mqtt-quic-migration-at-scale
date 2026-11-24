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

type quicListener struct {
	address   string
	tlsConfig *tls.Config

	listener  *quic.EarlyListener
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
}

func (listener *quicListener) ID() string       { return quicListenerID }
func (listener *quicListener) Address() string  { return listener.address }
func (listener *quicListener) Protocol() string { return "quic" }

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
	listener.listener, err = transport.ListenEarly(listener.tlsConfig, &quic.Config{
		MaxIdleTimeout: 120 * time.Second,
		Allow0RTT:      true,
	})
	return err
}

// Serve runs several accept loops: quic-go refuses handshaken connections when its accept queue (32) is full.
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

// serveConnection: the first bidirectional stream from the client carries the MQTT connection.
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

// quicStreamConn is the net.Conn that mochi-mqtt expects, made from a QUIC stream.
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
