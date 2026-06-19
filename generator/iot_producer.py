"""IoT message generator.

Publishes one JSON message per second to the Kafka topic iot_events,
following the interface contract (see README):

    {
      "type_id": <int 1..4>,
      "event_time": <epoch milliseconds>,
      "temperature": <float>,
      "humidity": <float>
    }

Temperature and humidity are spread per device type so that the
per-minute average and median computed downstream are meaningful.
"""

import json
import os
import random
import time

from kafka import KafkaProducer

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = os.environ.get("INPUT_TOPIC", "iot_events")

# Per type baselines (temperature_center, humidity_center).
# Each device type lives in a different value range so the aggregates differ.
TYPE_PROFILES = {
    1: (22.0, 45.0),
    2: (18.0, 70.0),
    3: (25.0, 50.0),
    4: (15.0, 80.0),
}


def build_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )


def make_event():
    type_id = random.choice(list(TYPE_PROFILES.keys()))
    temp_center, hum_center = TYPE_PROFILES[type_id]
    return {
        "type_id": type_id,
        "event_time": int(time.time() * 1000),
        "temperature": round(random.gauss(temp_center, 3.0), 2),
        "humidity": round(min(100.0, max(0.0, random.gauss(hum_center, 8.0))), 2),
    }


def main():
    producer = build_producer()
    print("producer started, sending to topic %s on %s" % (TOPIC, BOOTSTRAP_SERVERS))
    try:
        while True:
            event = make_event()
            producer.send(TOPIC, value=event)
            producer.flush()
            print("sent: %s" % json.dumps(event))
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("stopping producer")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
