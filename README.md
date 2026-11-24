# mqtt-tls-vs-quic-at-scale

MQTT over TCP + TLS 1.3 against MQTT over QUIC, with many publishers, in containers.
The broker, the client and the load are the same for the two transports. Only the transport changes.

## What a run does

Three containers start in this order:

1. **Broker**: mochi-mqtt with TLS 1.3 on TCP 8883 and MQTT over QUIC on UDP 14567 (quic-go).
   The two listeners use the same Go `crypto/tls` configuration.
2. **Subscriber**: one MQTT client on `bench/#`, on its own network without netem.
   It records the latency of each message.
3. **Publishers**: all in one container. Each publisher has its own connection, topic and source address.
   They connect at a fixed rate, then each one sends QoS 1 messages for 30 s, plus 3 s of padding.

The run stops when the publishers finish. The broker then saves its counters.

## Repository

| path | content |
| --- | --- |
| `broker/` | the MQTT broker |
| `gen/` | the load generator: `gen pub` (publishers) and `gen sub` (subscriber), on Eclipse Paho |
| `compose.yaml` | the three containers, their networks and the optional packet capture |
| `examples/` | one test per file: transport, publishers, message rate, payload size, network profile |
| `certs.sh` | makes the certificate of the broker |
| `summarize.py` | one csv row per run, with the numbers of the paper |

## Test files

A test file holds all the settings of one run. Edit it to change the test.

- **Network profile**: `QDISC` goes on the publisher link, at the two ends. `lan` has no impairment.
  The other profiles (4g, 3g, satellite-geo, wifi-bad) use netem, with their sources in the file.
- **Broker queue**: `MAX_PENDING` is the capacity of the outbound queue of each client.
- **Address change test**: `examples/lan-move.env`. Each publisher moves to a new source address two times.
  QUIC migrates its connection; TLS reconnects.

## Results

The results of a run go to the `RESULTS` folder of its test file:

- `sub.csv`: one row for each received message, with its latency.
- `pub.csv`: one row for each publisher, with the handshake and connect times.
- `pub.txt`: the settings of the run and the final counters.
- `broker-*`: CPU time, UDP counters, interface bytes and dropped messages of the broker.
- `move.csv`, `reconnect.csv`: only in the address change test.
- `broker.pcap`, `keys.log`: only with the capture.

A run is valid when all messages of the steady phase arrived, all publishers connected,
the time limit did not stop the run and the broker dropped no message.
In the address change test the delivery is a result, not a condition: TLS loses or repeats messages when it reconnects.

## Analyze the results

`./summarize.py results/*` prints one csv row per run. Compare a TLS run and a QUIC run of the same test:

- **Validity first**: use only rows with `valid` = True. A row with `cut` = True shows saturation:
  the time limit stopped the run, and `delivered_percent` shows how much arrived.
- **Delivery**: `received`, `expected` and `offered` messages, and `delivered_percent`.
- **Latency**: `latency_p50_ms` to `latency_max_ms`, from send to receive. The tail (p99, max) shows the stalls.
- **Connection setup**: `handshake_p50_ms` (TLS or QUIC handshake) and `connect_p95_ms` (until the MQTT CONNACK).
- **Cost**: `publisher_link_bytes_per_message` (both directions, handshakes included),
  `broker_cpu_seconds` and `broker_udp_errors` (UDP datagrams that the kernel dropped).

For the address change test, read the two extra files:

- `move.csv`: `DoneMs - StartMs` is the QUIC migration time of each publisher. TLS rows have no `DoneMs`.
- `reconnect.csv`: `ReconnectedMs - LostMs` is the TLS outage of each publisher. `ReconnectedMs` = 0 means no reconnection.

In Wireshark, open `broker.pcapng`:

- For TLS, turn on the TCP preference "Reassemble out-of-order segments" (`tcp.reassemble_out_of_order`).
- Wireshark counts retransmitted QUIC STREAM data two times in the MQTT counts.
- The capture keeps UDP segmentation offload on, so some broker-to-subscriber QUIC datagrams are too large to decode.

## Limits

- At most 8000 publishers: the neighbour table of the host has about 8192 entries.
- At most two moves in the address change test: at a third migration, the quic-go server has no free connection ID.

## Run a test

1. Make the certificate (one time):
   ```sh
   ./certs.sh
   ```
2. Run a test:
   ```sh
   docker compose --env-file examples/lan.env up --build --exit-code-from publisher
   ```
3. Run it again with `TRANSPORT=tls` in the file.
4. Capture: set `COMPOSE_PROFILES=capture` and `TLS_KEY_LOG=/results/keys.log` in the file.
5. Wireshark: put the keys in the capture, then open `broker.pcapng`:
   ```sh
   cd results/quic-200-lan
   editcap --inject-secrets tls,keys.log broker.pcap broker.pcapng
   ```
6. Summary, one row per run:
   ```sh
   ./summarize.py results/*
   ```
