# MQTT transport benchmark

Benchmark that compares MQTT over TCP + TLS 1.3 with MQTT over QUIC, with many publishers, in containers. The broker, the client and the load are the same for the two transports. Only the transport changes.

## Language

**Transport**:
The layer that carries MQTT between a client and the broker. The benchmark compares two: TCP + TLS 1.3, and QUIC.
_Avoid_: Protocol, channel, stack

**Broker**:
mochi-mqtt with a QUIC listener on quic-go (`broker/`). The two transports use the same Go `crypto/tls` configuration.
_Avoid_: Server, reference broker

**Publisher**:
One MQTT client connection with its own topic, its own socket and its own source IPv4 address. All publishers run in one container. Scale counts the publishers.
_Avoid_: Device, node, sender

**Subscriber**:
The single MQTT client that receives all publisher messages and records the latency of each one. Scale does not count it. Its link has no impairment.
_Avoid_: Consumer, receiver

**Scale**:
The number of publishers connected to the broker during a run. The methodology uses 200, 500, 1000, 2500 and 5000. The maximum on one host is 8000, because the neighbour table of the host (about 8192 entries) needs one entry for each publisher address.
_Avoid_: Load, size, number of devices

**Message rate**:
Messages per second sent by each publisher: 1 or 10 in the methodology.
_Avoid_: Throughput, frequency

**Network profile**:
The qdisc on the publisher link (`QDISC` in a test file), at the two ends of the link, so it acts once in each direction. `lan` has no impairment. The other profiles use netem (delay, jitter, loss) without a link rate.
_Avoid_: Scenario, condition, preset

**Test**:
One file in `examples/`: the transport, the scale, the message rate, the payload size, the network profile and the other settings of one run.
_Avoid_: Scenario, case, configuration

**Run**:
One execution of a test, in new containers: the connection storm, then the steady phase and the padding.
_Avoid_: Trial, iteration

**Connection storm**:
The start of a run: the publishers connect at a fixed rate (400 connections per second), before any message.
_Avoid_: Ramp-up, warm-up

**Steady phase**:
The 30 s of messages that must arrive for a valid run.
_Avoid_: Measurement window

**Padding**:
The 3 s of messages after the steady phase. They can be lost when the publishers stop.
_Avoid_: Tail, cool-down

**Outbound queue**:
The queue of messages that the broker keeps for each client (`MAX_PENDING`). When it is full, the broker drops a message.
_Avoid_: Buffer, inflight window

**Valid run**:
A run where all messages of the steady phase arrived, all publishers connected, the time limit did not stop the run and the broker dropped no message. In the address change test, the delivery is a result, not a condition.
_Avoid_: Successful run, clean run

**Address change test**:
A test where each publisher moves to a new source address during the steady phase, two times (`MOVE_AT`).
_Avoid_: Rebinding test, NAT test

**Migration**:
In the address change test, QUIC keeps the open connection and changes its path to the new address.
_Avoid_: Handover, roaming

**Reconnection**:
In the address change test, TCP cannot change its address. The client opens a new TCP and TLS connection from the new address. The time without a connection is the outage.
_Avoid_: Recovery, failover
