# One image for the broker, the publishers and the subscriber.

# Build stage: static binaries of the broker (refbroker) and the load generator (gen).
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY refbroker refbroker
COPY gen gen
RUN CGO_ENABLED=0 go build -o /out/ ./refbroker ./gen

# Runtime stage.
#   iproute2  tc (netem) and ip (publisher addresses)
#   tcpdump   packet capture in the broker's network namespace
#   ethtool   turns UDP segmentation offload off when a capture must be fully decodable
FROM ubuntu:24.04
RUN apt-get update \
 && apt-get install -y --no-install-recommends iproute2 tcpdump ethtool \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/refbroker /out/gen /usr/local/bin/
