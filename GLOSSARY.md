# Glossary

This benchmark compares MQTT over TCP + TLS 1.3 with MQTT over QUIC using many publishers running in containers. Both transports use the same broker, client implementation and workload.

The terms are organised from the most general to the most specific.

## Protocols and networking

- **IP address**: the network address assigned to an interface. A connection is identified by the addresses and ports at its two endpoints.
- **Socket**: a network endpoint in a program, bound to a local address and port.
- **TCP**: a reliable, ordered transport implemented by the kernel. A TCP connection is tied to its endpoint addresses and ports and cannot migrate to a new address.
- **UDP**: a connectionless datagram transport implemented by the kernel, with no delivery guarantee. QUIC runs over UDP.
- **TLS 1.3**: the security protocol that encrypts data and authenticates the server. For the TCP transport, TLS runs on top of TCP.
- **QUIC**: a transport protocol that runs over UDP and integrates TLS 1.3 (RFC 9000). It provides its own streams, loss recovery and connection IDs.
- **Handshake**: the initial exchange of messages that establishes a connection and its encryption keys. TCP + TLS 1.3 requires about two round trips; QUIC requires about one.
- **Session resumption (PSK)**: a client uses a session ticket from an earlier connection to resume a TLS 1.3 session without repeating the certificate exchange.
- **0-RTT**: with a suitable session ticket, the client can send data in its first flight, before the handshake completes. This is called early data.
- **Connection ID**: an identifier for a QUIC connection that allows it to survive a change of address.
- **Connection migration**: the movement of a QUIC connection to a new network address. The new path is validated using PATH_CHALLENGE and PATH_RESPONSE.
- **MQTT**: a publish and subscribe protocol for IoT. Clients publish messages on topics, and the broker forwards each message to the subscribers of that topic.
- **Topic**: the name under which an MQTT message is published, such as `bench/7`.
- **QoS 1**: MQTT delivery with an at-least-once guarantee. The broker acknowledges each published message with a PUBACK, and duplicate delivery is possible.
- **RTT**: round-trip time: the time it takes for a packet to reach the other endpoint and for a response to return.
- **netem**: the Linux network emulator. It adds delay, jitter and packet loss to traffic leaving an interface.
- **qdisc**: the queueing discipline of a Linux interface, such as netem or `noqueue`.
- **ARP and neighbour table**: the kernel uses ARP to resolve an IPv4 address on the local network to a hardware address. It stores the mapping in the neighbour table.
- **Container**: an isolated process with its own network namespace, including interfaces, addresses and routes. Docker runs the broker, publishers and subscriber in containers.

## The benchmark

- **Transport**: the layer that carries MQTT between a client and the broker. The benchmark compares TCP + TLS 1.3 and QUIC.
- **Broker**: mochi-mqtt with a QUIC listener built on quic-go (`broker/`). Both transports use the same Go `crypto/tls` configuration.
- **Publisher**: an MQTT client connection with its own topic, socket and source IPv4 address. All publishers run in one container.
- **Subscriber**: the single MQTT client that receives all publisher messages and records the latency of each one. Its link has no network impairment.
- **Network profile**: the qdisc configuration applied at both ends of the publisher link (`QDISC`). The `lan` profile has no impairment; the other profiles use netem to add delay, jitter and loss, without a link-rate limit.
- **Direct addresses**: publisher addresses on the publisher network. The broker resolves each address using ARP and therefore keeps one neighbour entry per publisher. These addresses are used in the methodology tests.
- **Routed addresses**: publisher addresses outside the directly connected networks. The broker routes the entire address block to the publisher container and therefore needs only one neighbour entry. These addresses are used in the address-change test.
- **Scale**: the number of publishers connected to the broker during a run: 200, 500, 1000, 2500 and 5000 in the methodology, with at most 8000 on one host.
- **Message rate**: messages per second sent by each publisher: 1 or 10 in the methodology.
- **Outbound queue**: the queue of messages that the broker keeps for each client (`BROKER_QUEUE_SIZE`). When it is full, the broker drops a message.
- **Test**: one file in `examples/`, containing all settings for one run.
- **Run**: one execution of a test in new containers: the connection storm, followed by the steady phase and the padding.
- **Connection storm**: the start of a run, when publishers connect at a fixed rate of 400 connections per second, before any messages are sent.
- **Steady phase**: the 30 s of messages that must arrive for a valid run.
- **Padding**: the 3 s of messages after the steady phase. These messages may be lost when the publishers stop.
- **Capture**: the packet capture in the broker’s network namespace (`broker.pcap`), with the TLS keys for Wireshark (`keys.log`).

## Measures

- **Latency**: the time from sending, as recorded by the publisher in the payload, to receipt at the subscriber.
- **Handshake time**: the duration of a publisher’s TLS or QUIC handshake.
- **Connect time**: the time from the start of a publisher’s connection to receipt of the broker’s MQTT CONNACK.
- **Offered, expected and received messages**: offered = messages sent, including padding; expected = messages from the steady phase; received = messages received by the subscriber.
- **Time limit (cut)**: a run that takes more than 120 s is stopped (`cut=true`). A cut run means that the broker could not process the load.
- **Valid run**: a run in which all messages from the steady phase arrived, all publishers connected, the time limit did not stop the run and the broker dropped no messages. The address change test does not require full delivery.

## Address change test

- **Address change test**: a test in which each publisher moves to a new source address during the steady phase.
- **Move**: one simultaneous address change for every publisher, at a time specified in `MOVE_AT_SECONDS`. Move k uses the k-th block of routed addresses.
- **Old address drop**: `DROP_OLD_ADDRESSES`. At each move, the previous addresses stop working, so the old path dies for every mechanism.
- **Mechanism**: how a publisher reaches the broker again after a move: TLS reconnection, QUIC reconnection (1-RTT or 0-RTT) or QUIC migration.
- **Migration**: QUIC keeps the existing connection open and moves it to the new address.
- **Reconnection**: the client opens a new connection from the new address and resumes the TLS 1.3 session: always for TLS, and for QUIC with `MOVE_MODE=reconnect` (1-RTT, or 0-RTT with `ZERO_RTT=true`).
- **Outage**: the time without a connection during a reconnection, from connection loss to reconnection.
