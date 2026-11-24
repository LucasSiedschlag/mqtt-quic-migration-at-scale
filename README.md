# mqtt-tls-vs-quic-at-scale

MQTT over TCP + TLS 1.3 vs. MQTT over QUIC, with many publishers, inside containers. Same broker, client and load for both transports.

## How a run works

1. **Broker**: starts `mochi-mqtt` with TLS 1.3 over TCP on port 8883, and MQTT over QUIC on UDP port 14567 (quic-go). Both listeners share the same `crypto/tls` config in Go.
2. **Subscriber**: runs one MQTT client on `bench/#`, on a separate network without any netem. It logs the latency for every message it receives.
3. **Publishers**: all run inside a single container. Each publisher gets its own connection, topic and source address. Publishers connect at a constant rate, then each publishes QoS 1 messages for 30 s, plus 3 s of padding.

The run finishes as soon as all publishers complete, then the broker dumps its counters to disk.

## Repo layout

| path | what it contains |
| --- | --- |
| `broker/` | the MQTT broker |
| `gen/` | load generator: `gen pub` (publishers) and `gen sub` (subscriber), on Paho |
| `compose.yaml` | defines the three containers, their networks and the packet capture (optional) |
| `examples/` | one test per file and network profile: `<profile>.env` (methodology tests) and `<profile>-move.env` (address change); `_template.env` lists every parameter for you to copy and adjust |
| `certs.sh` | generates the broker's certificate |
| `analysis/` | Python scripts to process results: `summarize.py` outputs one CSV row per run, with the figures referenced in the paper |
| `DETAILS.md` | describes constraints, edge cases and specific settings, explaining the why behind each choice |

## Test file

The test file controls every setting of a run. Modify it to change the test.

- **Network profile**: set `QDISC` on the publisher link, at both endpoints. `lan` has no impairment. Other profiles (4g, 3g, satellite-geo, wifi-bad) use netem; their sources are listed in the file.
- **Broker queue**: `BROKER_QUEUE_SIZE` sets the size of the outgoing queue for each client.
- **Address change test**: `examples/*-move.env` files. Publishers move their source address at the times in `MOVE_AT_SECONDS` (at most three moves). QUIC migrates the connection; TLS reconnects.

## Result files

Each run writes its output to the `RESULTS_FOLDER` specified in the test file:

- `sub.csv`: one row per message received, along with its latency.
- `pub.csv`: one row per publisher, containing handshake and connect times.
- `pub.txt`: run settings and final counters.
- `broker-*`: broker CPU usage, UDP counters, interface traffic and dropped messages.
- `move.csv`, `reconnect.csv`: generated only during the address change test.
- `broker.pcap`, `keys.log`: created only if the capture is enabled.

A run is considered valid if every steady-phase message was delivered, all publishers connected, the time limit was not hit and the broker did not drop any message. For the address change test, full delivery is not required, since TLS can lose or duplicate messages during reconnection.

## Process the results

`./analysis/summarize.py results/*` prints one CSV row per run. Compare a TLS run to a QUIC run within the same test:

- **First filter by validity**: select only rows where `valid` = True. A row with `cut` = True indicates saturation: the time limit ended the run, and `delivered_percent` shows how much of the traffic got through.
- **Delivery metrics**: count of `received`, `expected` and `offered` messages, and `delivered_percent`.
- **Latency**: `latency_p50_ms` to `latency_max_ms`, measured from send to receive. The tail (p99, max) reveals stalls.
- **Connection setup**: `handshake_p50_ms` (time for TLS or QUIC handshake) and `connect_p95_ms` (time until MQTT CONNACK).
- **Resource consumption**: `publisher_link_bytes_per_message` (bidirectional, including handshakes), `broker_cpu_seconds` and `broker_udp_errors` (UDP datagrams dropped by the kernel).

In the address change test, the summary also reads the additional CSV files:

- `migration_p50_ms` to `migration_p99_ms`: from `move.csv`, `DoneMs - StartMs` is the time each publisher took to migrate its QUIC connection. Reconnection rows do not have a `DoneMs`.
- `outage_p50_ms` to `outage_p99_ms`: from `reconnect.csv`, `ReconnectedMs - LostMs` is the time each publisher stayed without a connection when it reconnects (TLS, or QUIC with `MOVE_MODE=reconnect`).
- `not_reconnected`: rows with `ReconnectedMs` = 0, the publisher did not reconnect.

See [DETAILS.md](DETAILS.md) for a list of limitations, Wireshark configuration and explanations for specific settings.

## Execute a test

1. Generate the certificate (once):
   ```sh
   ./certs.sh
   ```
2. Run a test:
   ```sh
   docker compose --env-file examples/lan.env up --build --exit-code-from publisher
   ```
3. Repeat the test with `TRANSPORT=tls` in the file.
4. Capture: remove the `#` of the three capture lines in the file.
5. Add the TLS keys to the capture and inspect it in Wireshark:
   ```sh
   cd results/quic-200-lan
   editcap --inject-secrets tls,keys.log broker.pcap broker.pcapng
   open broker.pcapng
   ```
6. Print a summary, one row per run:
   ```sh
   ./analysis/summarize.py results/*
   ```
