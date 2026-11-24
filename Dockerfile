# One image for the broker, the publishers and the subscriber.

# Build stage: makes static binaries of the broker and of the load generator (gen).
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY broker broker
COPY gen gen
RUN CGO_ENABLED=0 go build -o /out/ ./broker ./gen

# Runtime stage.
#   iproute2  tc (netem) and ip (publisher addresses)
#   tcpdump   packet capture in the network namespace of the broker
#   ethtool   turns off UDP segmentation offload, for a capture that Wireshark can decode fully
FROM ubuntu:24.04
RUN apt-get update \
 && apt-get install -y --no-install-recommends iproute2 tcpdump ethtool \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/broker /out/gen /usr/local/bin/
