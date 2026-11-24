#!/usr/bin/env python3
"""Usage: ./analysis/summarize.py <results folder>...      for example: ./analysis/summarize.py results/*

Prints one csv row for each run. README.md has the columns and the validity rule.
"""

import csv
import os
import sys

COLUMNS = [
    "run",
    "transport",
    "publishers",
    "message_rate",
    "payload_size",
    "move_at",
    "move_mode",
    "zero_rtt",
    "move_drop",
    "received",
    "expected",
    "offered",
    "delivered_percent",
    "connect_failed",
    "broker_messages_dropped",
    "cut",
    "valid",
    "latency_p50_ms",
    "latency_p95_ms",
    "latency_p99_ms",
    "latency_max_ms",
    "handshake_p50_ms",
    "connect_p95_ms",
    "broker_cpu_seconds",
    "broker_udp_errors",
    "publisher_link_bytes_per_message",
    "outage_p50_ms",
    "outage_p95_ms",
    "outage_p99_ms",
    "not_reconnected",
    "migration_p50_ms",
    "migration_p95_ms",
    "migration_p99_ms",
]


def percentile(sorted_values, percent):
    """Nearest-rank percentile of a sorted list; None for an empty list."""
    if not sorted_values:
        return None
    rank = min(len(sorted_values) - 1, int(percent / 100 * len(sorted_values)))
    return sorted_values[rank]


def read_settings(folder):
    with open(os.path.join(folder, "pub.txt")) as file:
        return dict(line.rstrip("\n").split("=", 1) for line in file if "=" in line)


def read_column(path, column):
    with open(path) as file:
        return sorted(int(row[column]) for row in csv.DictReader(file))


def broker_messages_dropped(folder):
    with open(os.path.join(folder, "broker-messages-dropped")) as file:
        return int(file.read())


def broker_cpu_seconds(folder):
    with open(os.path.join(folder, "broker-cpu.stat")) as file:
        for line in file:
            name, value = line.split()
            if name == "usage_usec":
                return int(value) / 1_000_000
    return None


def broker_udp_errors(folder):
    with open(os.path.join(folder, "broker-snmp")) as file:
        udp_lines = [line.split()[1:] for line in file if line.startswith("Udp:")]
    names, values = udp_lines  # first line: counter names; second line: counter values
    return int(dict(zip(names, values))["InErrors"])


def publisher_link_bytes(folder):
    with open(os.path.join(folder, "broker-dev")) as file:
        for line in file:
            interface, _, counters = line.partition(":")
            if interface.strip() == "pub0":
                fields = counters.split()
                return int(fields[0]) + int(fields[8])  # receive bytes, transmit bytes
    return None


def address_change_times(folder):
    """Outage times (reconnect.csv), the count of publishers that did not reconnect, and migration times (move.csv), in ms."""
    outages, not_reconnected, migrations = [], 0, []
    reconnect_path = os.path.join(folder, "reconnect.csv")
    if os.path.exists(reconnect_path):
        with open(reconnect_path) as file:
            for row in csv.DictReader(file):
                if int(row["ReconnectedMs"]) == 0:
                    not_reconnected += 1
                else:
                    outages.append(int(row["ReconnectedMs"]) - int(row["LostMs"]))
    move_path = os.path.join(folder, "move.csv")
    if os.path.exists(move_path):
        with open(move_path) as file:
            for row in csv.DictReader(file):
                # TLS and QUIC reconnection rows have no DoneMs
                if row["DoneMs"] and int(row["DoneMs"]) > 0:
                    migrations.append(int(row["DoneMs"]) - int(row["StartMs"]))
    return sorted(outages), not_reconnected, sorted(migrations)


def summarize(folder):
    settings = read_settings(folder)
    publishers = int(settings["publishers"])
    message_rate = int(settings["message_rate"])
    steady_seconds = int(settings["steady_seconds"])
    padding_seconds = int(settings["padding_seconds"])

    latencies = read_column(os.path.join(folder, "sub.csv"), "Latency")
    handshakes = read_column(os.path.join(folder, "pub.csv"), "Handshake")
    connects = read_column(os.path.join(folder, "pub.csv"), "Connect")

    received = len(latencies)
    expected = publishers * message_rate * steady_seconds
    offered = publishers * message_rate * (steady_seconds + padding_seconds)
    connect_failed = int(settings["connect_failed"])
    cut = settings["cut"] == "true"
    dropped = broker_messages_dropped(folder)
    move_at = settings.get("move_at", "")
    if move_at:
        address_change_files = ("move.csv", "reconnect.csv")
        all_files = all(
            os.path.exists(os.path.join(folder, name)) for name in address_change_files
        )
        valid = connect_failed == 0 and not cut and dropped == 0 and all_files
    else:
        valid = (
            expected <= received <= offered
            and connect_failed == 0
            and not cut
            and dropped == 0
        )
    link_bytes = publisher_link_bytes(folder)
    bytes_per_message = (
        round(link_bytes / received) if link_bytes and received else None
    )
    outages, not_reconnected, migrations = address_change_times(folder)

    return {
        "run": os.path.basename(os.path.normpath(folder)),
        "transport": settings["transport"],
        "publishers": publishers,
        "message_rate": message_rate,
        "payload_size": settings["payload_size"],
        "move_at": move_at,
        "move_mode": settings.get("move_mode", ""),
        "zero_rtt": settings.get("zero_rtt", ""),
        "move_drop": settings.get("move_drop", ""),
        "received": received,
        "expected": expected,
        "offered": offered,
        "delivered_percent": round(100 * received / offered, 2),
        "connect_failed": connect_failed,
        "broker_messages_dropped": dropped,
        "cut": cut,
        "valid": valid,
        "latency_p50_ms": percentile(latencies, 50),
        "latency_p95_ms": percentile(latencies, 95),
        "latency_p99_ms": percentile(latencies, 99),
        "latency_max_ms": latencies[-1] if latencies else None,
        "handshake_p50_ms": percentile(handshakes, 50),
        "connect_p95_ms": percentile(connects, 95),
        "broker_cpu_seconds": broker_cpu_seconds(folder),
        "broker_udp_errors": broker_udp_errors(folder),
        "publisher_link_bytes_per_message": bytes_per_message,
        "outage_p50_ms": percentile(outages, 50),
        "outage_p95_ms": percentile(outages, 95),
        "outage_p99_ms": percentile(outages, 99),
        "not_reconnected": not_reconnected if move_at else None,
        "migration_p50_ms": percentile(migrations, 50),
        "migration_p95_ms": percentile(migrations, 95),
        "migration_p99_ms": percentile(migrations, 99),
    }


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    writer = csv.DictWriter(sys.stdout, fieldnames=COLUMNS)
    writer.writeheader()
    for folder in sys.argv[1:]:
        try:
            writer.writerow(summarize(folder))
        # not a run folder, or a run that did not write its files
        except OSError as error:
            print(f"skipping {folder}: {error}", file=sys.stderr)
