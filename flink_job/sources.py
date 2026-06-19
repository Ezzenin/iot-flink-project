"""Source construction.

Two sources, deliberately built through two different APIs so the job
demonstrates both a DataStream source and a Table/SQL source:

- iot_events is read with the DataStream KafkaSource, then parsed into a
  typed Row. This is the side that later crosses into the Table API.
- device_types is declared as a JDBC table through the Table/SQL API.
"""

import json

from pyflink.common import Types, WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)

import config


# Row layout for a parsed IoT event. Field order and names are reused when the
# stream is handed to the Table API, so keep them in sync with the schema there.
EVENT_ROW_TYPE = Types.ROW_NAMED(
    ["type_id", "event_time", "temperature", "humidity"],
    [Types.INT(), Types.LONG(), Types.DOUBLE(), Types.DOUBLE()],
)


def _parse_event(raw):
    """Parse one JSON string from Kafka into a typed Row.

    The contract guarantees the four fields; anything malformed is turned into
    a row with a sentinel type_id of -1 so it can be filtered downstream
    instead of killing the job.
    """
    from pyflink.common import Row

    try:
        obj = json.loads(raw)
        return Row(
            int(obj["type_id"]),
            int(obj["event_time"]),
            float(obj["temperature"]),
            float(obj["humidity"]),
        )
    except (ValueError, KeyError, TypeError):
        return Row(-1, 0, 0.0, 0.0)


def build_kafka_stream(env):
    """Create the iot_events DataStream of parsed, typed Rows.

    Event time and watermark are intentionally NOT assigned here. They are
    declared on the Table side in from_data_stream, derived from the
    event_time field, which keeps the event-time definition in one place.
    """
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(config.KAFKA_BOOTSTRAP)
        .set_topics(config.INPUT_TOPIC)
        .set_group_id(config.CONSUMER_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_stream = env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "kafka_iot_events",
    )

    parsed = raw_stream.map(_parse_event, output_type=EVENT_ROW_TYPE)
    # type_id is the first field; index positionally since the runtime Row
    # passed to Python functions does not carry field names.
    return parsed.filter(lambda r: r[0] != -1)


def register_device_types_table(t_env):
    """Declare device_types as a JDBC source table on the Table/SQL API.

    The JDBC connector source is bounded, which is exactly what a static
    reference table needs.
    """
    t_env.execute_sql(
        """
        CREATE TABLE device_types (
            id INT,
            type_name STRING,
            PRIMARY KEY (id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '%s',
            'table-name' = '%s',
            'username' = '%s',
            'password' = '%s'
        )
        """
        % (config.PG_JDBC_URL, config.PG_TABLE, config.PG_USER, config.PG_PASSWORD)
    )
