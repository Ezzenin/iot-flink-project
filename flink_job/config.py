"""Single source of truth for names, addresses and paths.

All values come from the interface contract (see README).
The job runs inside a container on the compose network, so the defaults
point at the internal service hostnames (kafka, postgres). They can be
overridden through environment variables, which keeps the same code usable
from a host venv (set KAFKA_BOOTSTRAP=localhost:9092, PG_HOST=localhost).
"""

import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JARS_DIR = os.environ.get("JARS_DIR", os.path.join(PROJECT_ROOT, "jars"))

# Kafka
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:29092")
INPUT_TOPIC = os.environ.get("INPUT_TOPIC", "iot_events")
OUTPUT_TOPIC = os.environ.get("OUTPUT_TOPIC", "iot_minute_agg")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "flink_iot_job")

# Postgres
PG_HOST = os.environ.get("PG_HOST", "postgres")
PG_PORT = int(os.environ.get("PG_PORT", "5432"))
PG_DB = os.environ.get("PG_DB", "testdb")
PG_USER = os.environ.get("PG_USER", "postgres")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "postgres")
PG_TABLE = os.environ.get("PG_TABLE", "device_types")
PG_JDBC_URL = "jdbc:postgresql://%s:%d/%s" % (PG_HOST, PG_PORT, PG_DB)

# Allowed out-of-orderness for event time watermarks, in seconds.
WATERMARK_DELAY = os.environ.get("WATERMARK_DELAY", "5")


def jar_paths():
    """Return absolute file:// URIs for every jar in JARS_DIR."""
    if not os.path.isdir(JARS_DIR):
        return []
    uris = []
    for name in sorted(os.listdir(JARS_DIR)):
        if name.endswith(".jar"):
            uris.append("file://" + os.path.join(JARS_DIR, name))
    return uris
