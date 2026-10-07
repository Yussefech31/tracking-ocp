import argparse
import json
import os
import sys
from kafka import KafkaConsumer
from kafka_producer import run_producer


def verify_stream(bootstrap_servers, topic_name, sample_size=10):
    run_producer(
        bootstrap_servers=bootstrap_servers,
        topic_name=topic_name,
        num_vehicles=5,
        interval=0.1,
        max_events=sample_size,
    )

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=5000,
    )

    required_fields = {
        "vehicle_id": str,
        "timestamp": str,
        "latitude": float,
        "longitude": float,
        "speed_kmh": (float, int),
        "fuel_level": (float, int),
        "engine_temperature": (float, int),
    }

    records_read = 0
    for message in consumer:
        payload = message.value
        for field, expected_type in required_fields.items():
            if field not in payload:
                raise ValueError(
                    f"Missing required field '{field}' in payload: {payload}"
                )
            if not isinstance(payload[field], expected_type):
                raise TypeError(
                    f"Field '{field}' expected {expected_type}, got {type(payload[field])}"
                )

        if not (28.0 <= payload["latitude"] <= 36.5):
            raise ValueError(
                f"Latitude out of Moroccan geographic bounds: {payload['latitude']}"
            )
        if not (-13.5 <= payload["longitude"] <= -1.0):
            raise ValueError(
                f"Longitude out of Moroccan geographic bounds: {payload['longitude']}"
            )
        if payload["speed_kmh"] < 0:
            raise ValueError(f"Speed cannot be negative: {payload['speed_kmh']}")
        if not (0 <= payload["fuel_level"] <= 100):
            raise ValueError(
                f"Fuel level out of percentage bounds: {payload['fuel_level']}"
            )

        records_read += 1
        if records_read >= sample_size:
            break

    consumer.close()
    if records_read == 0:
        raise RuntimeError("No records were consumed from the Kafka topic.")
    print(
        f"Validation successful: {records_read} telemetry events verified with valid schema."
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"),
    )
    parser.add_argument(
        "--topic", default=os.getenv("KAFKA_TOPIC", "vehicle_telemetry")
    )
    parser.add_argument("--sample-size", type=int, default=10)
    args = parser.parse_args()

    try:
        verify_stream(args.bootstrap_servers, args.topic, args.sample_size)
    except Exception as exc:
        print(f"Validation failed: {exc}", file=sys.stderr)
        sys.exit(1)
