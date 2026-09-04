"""
Assignment 2 - Kafka Producer
Streaming sample NSE equity candle data to the nse-equity-ticks topic.

Data source/schema follows Assignment 1:
symbol, timestamp, date, open, high, low, close, volume,
prev_close, pct_change, exchange.

The source is simulated from the Assignment 1 yfinance schema so the
Kafka stream can be demonstrated without depending on live market hours.
"""

import argparse
import csv
import json
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError


DEFAULT_TOPIC = "nse-equity-ticks"
DEFAULT_BROKER = "localhost:9092"


def ensure_topic(broker: str, topic: str, partitions: int = 3) -> None:
    """Create the topic if it does not already exist."""
    admin = KafkaAdminClient(bootstrap_servers=broker, client_id="sda-assignment2-admin")
    try:
        admin.create_topics([
            NewTopic(
                name=topic,
                num_partitions=partitions,
                replication_factor=1,
            )
        ])
        print(f"Created topic '{topic}' with {partitions} partitions (RF=1 for a local demo).")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic}' already exists. Using the existing configuration.")
    finally:
        admin.close()


def load_rows(csv_file: Path):
    with csv_file.open("r", encoding="utf-8", newline="") as f:
        yield from csv.DictReader(f)


def clean_row(row: dict) -> dict:
    """Convert CSV strings back to the schema's intended types."""
    return {
        "symbol": row["symbol"],
        "timestamp": row["timestamp"],
        "date": row["date"],
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": int(row["volume"]),
        "prev_close": float(row["prev_close"]),
        "pct_change": float(row["pct_change"]),
        "exchange": row["exchange"],
    }


def main():
    parser = argparse.ArgumentParser(description="Stream sample equity data to Kafka.")
    parser.add_argument("--broker", default=DEFAULT_BROKER)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument(
        "--file",
        default=str(Path(__file__).with_name("sample_market_data.csv")),
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Seconds between messages. Use 0 for maximum replay speed.",
    )
    parser.add_argument(
        "--loops",
        type=int,
        default=1,
        help="Number of times to replay the sample dataset.",
    )
    args = parser.parse_args()

    csv_file = Path(args.file)
    if not csv_file.exists():
        raise FileNotFoundError(f"Dataset not found: {csv_file}")

    ensure_topic(args.broker, args.topic, partitions=3)

    producer = KafkaProducer(
        bootstrap_servers=args.broker,
        acks="all",
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )

    sent = 0
    print(f"\nStreaming {csv_file.name} -> {args.topic}")
    print(f"Broker: {args.broker} | Delay: {args.delay}s | Loops: {args.loops}\n")

    try:
        for loop in range(args.loops):
            for raw in load_rows(csv_file):
                message = clean_row(raw)
                future = producer.send(
                    args.topic,
                    key=message["symbol"],
                    value=message,
                )
                metadata = future.get(timeout=10)
                sent += 1

                print(
                    f"[{sent:03d}] "
                    f"{message['timestamp']} | "
                    f"{message['symbol']:<14} | "
                    f"close={message['close']:>8.2f} | "
                    f"pct={message['pct_change']:>7.2f}% | "
                    f"partition={metadata.partition} offset={metadata.offset}"
                )
                if args.delay > 0:
                    time.sleep(args.delay)
    except KeyboardInterrupt:
        print("\nProducer stopped by user.")
    finally:
        producer.flush()
        producer.close()

    print(f"\nDone. Total messages sent: {sent}")


if __name__ == "__main__":
    main()
