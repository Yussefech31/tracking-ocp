import argparse
import json
import math
import os
import random
import time
from datetime import datetime, timezone
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

WAYPOINTS = [
    {"name": "Casablanca", "lat": 33.5731, "lon": -7.5898},
    {"name": "Jorf Lasfar", "lat": 33.1256, "lon": -8.6256},
    {"name": "Safi", "lat": 32.2994, "lon": -9.2372},
    {"name": "Khouribga", "lat": 32.8811, "lon": -6.9063},
    {"name": "Benguerir", "lat": 32.2359, "lon": -7.9542},
    {"name": "Marrakech", "lat": 31.6295, "lon": -7.9811},
    {"name": "Rabat", "lat": 34.0209, "lon": -6.8416},
    {"name": "Kenitra", "lat": 34.2610, "lon": -6.5802},
]


class VehicleSimulator:
    def __init__(self, vehicle_id):
        self.vehicle_id = vehicle_id
        start_wp, target_wp = random.sample(WAYPOINTS, 2)
        self.lat = start_wp["lat"]
        self.lon = start_wp["lon"]
        self.target_lat = target_wp["lat"]
        self.target_lon = target_wp["lon"]
        self.speed_kmh = random.uniform(50.0, 85.0)
        self.fuel_level = random.uniform(40.0, 95.0)
        self.engine_temperature = random.uniform(82.0, 92.0)
        self.status = "MOVING"

    def step(self):
        anomaly_roll = random.random()
        if anomaly_roll < 0.03:
            self.speed_kmh = random.uniform(105.0, 125.0)
            self.engine_temperature = random.uniform(102.0, 115.0)
            self.status = "OVERSPEED"
        elif anomaly_roll < 0.05:
            self.speed_kmh = 0.0
            self.engine_temperature = max(65.0, self.engine_temperature - 1.5)
            self.status = "STOPPED"
        elif anomaly_roll < 0.07:
            self.engine_temperature = random.uniform(105.0, 118.0)
            self.speed_kmh = max(20.0, self.speed_kmh - 10.0)
            self.status = "OVERHEATING"
        else:
            self.status = "MOVING"
            self.speed_kmh = max(
                30.0, min(95.0, self.speed_kmh + random.uniform(-4.0, 4.0))
            )
            self.engine_temperature = max(
                80.0, min(96.0, self.engine_temperature + random.uniform(-0.8, 0.8))
            )

        d_lat = self.target_lat - self.lat
        d_lon = self.target_lon - self.lon
        distance = math.hypot(d_lat, d_lon)

        if distance < 0.02:
            new_target = random.choice(
                [wp for wp in WAYPOINTS if wp["lat"] != self.target_lat]
            )
            self.target_lat = new_target["lat"]
            self.target_lon = new_target["lon"]
        else:
            step_fraction = (self.speed_kmh / 3600.0) * 0.01 / max(distance, 0.001)
            self.lat += d_lat * min(step_fraction, 0.05)
            self.lon += d_lon * min(step_fraction, 0.05)

        fuel_consumption = (
            (self.speed_kmh / 100.0) * 0.05 if self.speed_kmh > 0 else 0.01
        )
        self.fuel_level -= fuel_consumption
        if self.fuel_level < 5.0:
            self.fuel_level = random.uniform(85.0, 100.0)

        return {
            "vehicle_id": self.vehicle_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "latitude": round(self.lat, 6),
            "longitude": round(self.lon, 6),
            "speed_kmh": round(self.speed_kmh, 2),
            "fuel_level": round(self.fuel_level, 2),
            "engine_temperature": round(self.engine_temperature, 2),
        }


def ensure_topic_exists(
    bootstrap_servers, topic_name, num_partitions=3, replication_factor=1
):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    existing_topics = admin_client.list_topics()
    if topic_name not in existing_topics:
        new_topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
    admin_client.close()


def run_producer(
    bootstrap_servers, topic_name, num_vehicles=20, interval=1.0, max_events=None
):
    ensure_topic_exists(bootstrap_servers, topic_name)
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
        retries=3,
    )

    fleet = [VehicleSimulator(f"V{i:04d}") for i in range(1, num_vehicles + 1)]
    event_count = 0

    try:
        while True:
            for vehicle in fleet:
                event = vehicle.step()
                producer.send(topic_name, key=event["vehicle_id"], value=event)
                event_count += 1
                if max_events is not None and event_count >= max_events:
                    producer.flush()
                    producer.close()
                    return event_count
            producer.flush()
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()

    return event_count


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094"),
    )
    parser.add_argument(
        "--topic", default=os.getenv("KAFKA_TOPIC", "vehicle_telemetry")
    )
    parser.add_argument("--num-vehicles", type=int, default=20)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--max-events", type=int, default=None)
    args = parser.parse_args()

    total = run_producer(
        bootstrap_servers=args.bootstrap_servers,
        topic_name=args.topic,
        num_vehicles=args.num_vehicles,
        interval=args.interval,
        max_events=args.max_events,
    )
    print(f"Produced {total} events to topic '{args.topic}'")
