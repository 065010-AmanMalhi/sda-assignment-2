# Streaming Data Analytics - Assignment 2

## Files
- `sample_market_data.csv` - 60 sample one-minute NSE equity candles based on the Assignment 1 schema.
- `producer.py` - Kafka producer that publishes each row as JSON, keyed by `symbol`.
- `sample_market_data.jsonl` - same sample records in JSON Lines format.
- `requirements.txt` - Python dependency.

## Kafka configuration
- Topic: `nse-equity-ticks`
- Partitions: 3
- Producer acknowledgements: `acks=all`
- Kafka message key: stock symbol
- Message value: JSON
- Local demo replication factor: 1. A 3-broker Kafka cluster can use RF=3.

## Run
1. Make sure Kafka is running on `localhost:9092`.
2. Install the dependency:
   `pip install -r requirements.txt`
3. Start the producer:
   `python producer.py`
4. For a visible Kafka consumer terminal, run:
   `kafka-console-consumer --bootstrap-server localhost:9092 --topic nse-equity-ticks --from-beginning --property print.key=true --property key.separator=" | "`

The producer prints each message's symbol, close, percentage change, partition and offset, which is useful for the required terminal screenshot.

The sample includes deliberately elevated percentage moves for demonstration of the circuit-monitoring use case. It is simulated data, not live market data.
