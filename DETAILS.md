# Details

Limitations, exceptions, and particular benchmark settings, with the explanation for each one.

## Network profile (netem)

- `QDISC` is applied once in each direction on `pub0` at both ends of the publisher link: the publisher container and the broker. No netem is applied to the subscriber link.
- When a source specifies an RTT, the one-way delay is set to RTT/2.
- Without a `distribution` option, netem jitter is uniformly distributed over MEAN ± JITTER. With `distribution normal`, JITTER is the standard deviation.
- No link rate is configured because the benchmark measures bandwidth.
- `limit 100000` increases the queue capacity from the default of 1,000 packets. If delay × packet rate exceeds the queue capacity, the queue overflows, causing packet loss that the profile does not specify.
- `lan` uses `noqueue`, the default qdisc for a Docker interface, and introduces no impairment.

## Broker outbound queue

- mochi-mqtt gives each client an outbound queue of `BROKER_QUEUE_SIZE` messages. Because the subscriber receives every message from every publisher, a small queue fills up at scale, causing the broker to disconnect the subscriber or drop messages.
- The methodology tests use `1048576` (`1 << 20`).
- mochi-mqtt provides equal capacity to all clients, including publishers: 8 MiB per client at `1 << 20`. This memory becomes resident after a reconnection, but not after the first connection, so the address change test uses a smaller queue.
- The address change test uses `65536`.
- When the count changes, the broker logs `messages dropped N` and writes the total to `broker-messages-dropped`. A valid run has 0 dropped messages.

## Publisher addresses

- Each publisher binds to its own source address. A local route allows it to bind to any address in a block without adding each address individually.
- Direct (methodology tests): the block is on the publisher network, so the broker maintains one neighbour (ARP) entry per publisher. The host’s neighbour table holds about 8192 entries, limiting each run to 8000 publishers.
- Routed (address-change test): the block is outside the networks. The broker routes the entire block to the publisher container, so it maintains only one neighbour entry. Each publisher can use four addresses: the initial address and three moves.

## Address change test

- At move k, publisher i moves to the first address + i + 16384 × k. Each move uses a separate /18 block.
- At most three moves: the routed `/16` has four `/18` blocks (start and three moves).
- All publishers move at the same time: a single goroutine signals each move.
- TLS move: the TCP socket’s deadline is set in the past, causing every read and write to fail. Calling Close does not work: Paho ignores the "use of closed network connection" error because it expects that error after its own Close.
- Old addresses (`DROP_OLD_ADDRESSES`): tc filters on `pub0` drop packets to and from the previous block. tc drops packets after they leave the socket, so the sender receives no error, as on a real network. An nftables drop returns EPERM to `sendmsg`, and quic-go closes the connection after a send error. This causes QUIC migration to fail for a reason that would not occur on a real network.
- A valid test run does not require full delivery: TLS may lose or duplicate messages when reconnecting due to QoS 1 resends, so delivery is one of the measured results.

## QUIC

- quic-go is v0.66.0: the client answers a PATH_CHALLENGE on the path where it arrived, which a migration with the old address dropped needs.
- The first bidirectional stream opened by the client carries the MQTT connection.
- quic-go rejects a connection after the handshake completes if its accept queue (32 entries) is full. A single accept loop cannot drain the queue during a connection storm, so the broker runs several loops in parallel.
- `quic.Dial` uses zero-length connection IDs, which prevent migration. Only `MOVE_MODE=migrate` dials with 4-byte connection IDs, so the other runs keep zero-length connection IDs.
- 0-RTT: the broker accepts early data (`Allow0RTT`). A client sends its MQTT CONNECT as early data only when `ZERO_RTT=true` and it has a session ticket from an earlier connection. Therefore, only reconnections can use 0-RTT.
- Each publisher retains its TLS session tickets, allowing TLS or QUIC reconnections to resume the session using PSK without sending the certificate.

## Sending and timing

- A publisher sends message m at start + m / rate. It does not wait for the previous message’s PUBACK, so the offered rate is independent of the round-trip time.
- A message that has not received a PUBACK after 10 s counts as failed.
- A run stops if it takes more than 120 s (`cut=true` in `pub.txt`). This happens when the broker cannot process the load. The run’s data is retained.
- `pub.csv` is written when the connection phase ends, so it is retained even if the run stops later.
- After the publishers stop, the subscriber continues until no message has arrived for 2 s, because the broker may still be delivering messages.
- Times in the CSV files are Unix milliseconds. The publishers, subscriber, and capture use the same kernel clock, so their timestamps can be compared directly. Time since the start of a run = column - `started_at_ms`.
- The broker counters (`broker-*`) start when the container starts, so they include the few packets sent before the first publisher.

## Capture

- tcpdump captures the first `CAPTURE_PACKETS` packets on the MQTT ports (TCP 8883 and UDP 14567), excluding loopback traffic.
- UDP segmentation offload remains enabled, so some broker-to-subscriber QUIC datagrams are too large for Wireshark to decode. Publisher-to-broker traffic and all TLS traffic decode fully.
- To decode TLS, enable Wireshark’s TCP preference for reassembling out-of-order segments (`tcp.reassemble_out_of_order`).
- Wireshark counts retransmitted QUIC STREAM data twice in its MQTT counts.

## Test files and Compose

- Put each comment in a test file on its own line: Compose treats a comment after an empty value as the value.
- A line starting with `#` is disabled. The capture settings and alternatives in `_template.env` use this convention.
- Routes for publisher addresses use `${VAR:+...}`: Compose adds a route only when the test file sets the corresponding block.
- The containers have fixed names (`broker`, `subscriber`, `publisher`, and `capture`), so only one run at a time can use the machine.
