"""Pipeline wiring.

This is where the DataStream side crosses into the Table/SQL side and the rest
of the logic (enrichment join, windowed aggregation, sink) is expressed in SQL.

Flow:
    DataStream(iot events) --from_data_stream--> Table iot (event time + watermark)
    iot  LOOKUP JOIN  device_types (JDBC)  ->  enriched (type_name added)
    enriched  TUMBLE 1 min  ->  AVG(temperature), median(humidity)
    result  INSERT INTO  iot_minute_agg (Kafka sink table)
"""

from pyflink.table import DataTypes, Schema

import config
from sources import register_device_types_table
from udf import median


def to_iot_table(t_env, iot_stream):
    """Cross from DataStream to Table and register the iot view.

    The schema is declared explicitly. Without it PyFlink loses field names and
    types when it serializes the Row. The event-time column and the watermark
    are derived here from the epoch-millis event_time field, and a processing
    time column is added for the lookup join.
    """
    schema = (
        Schema.new_builder()
        .column("type_id", DataTypes.INT())
        .column("event_time", DataTypes.BIGINT())
        .column("temperature", DataTypes.DOUBLE())
        .column("humidity", DataTypes.DOUBLE())
        .column_by_expression("ts_event", "TO_TIMESTAMP_LTZ(event_time, 3)")
        .column_by_expression("proctime", "PROCTIME()")
        .watermark("ts_event", "ts_event - INTERVAL '" + config.WATERMARK_DELAY + "' SECOND")
        .build()
    )

    iot_table = t_env.from_data_stream(iot_stream, schema)
    t_env.create_temporary_view("iot", iot_table)


def create_enriched_view(t_env):
    """Enrich events with type_name via a lookup join on the JDBC reference table.

    FOR SYSTEM_TIME AS OF the processing time keeps the event-time attribute
    ts_event on the left side, so the windowed aggregation downstream can still
    use it as the rowtime.
    """
    t_env.execute_sql(
        """
        CREATE TEMPORARY VIEW enriched AS
        SELECT
            i.ts_event       AS ts_event,
            d.type_name      AS type_name,
            i.temperature    AS temperature,
            i.humidity       AS humidity
        FROM iot AS i
        LEFT JOIN device_types FOR SYSTEM_TIME AS OF i.proctime AS d
            ON i.type_id = d.id
        """
    )


def create_sink_table(t_env):
    """Declare the Kafka output table on the Table/SQL API."""
    t_env.execute_sql(
        """
        CREATE TABLE iot_minute_agg (
            window_time      STRING,
            type_name        STRING,
            avg_temperature  DOUBLE,
            median_humidity  DOUBLE
        ) WITH (
            'connector' = 'kafka',
            'topic' = '%s',
            'properties.bootstrap.servers' = '%s',
            'format' = 'json'
        )
        """
        % (config.OUTPUT_TOPIC, config.KAFKA_BOOTSTRAP)
    )


def aggregation_sql():
    """Per-minute aggregation: average temperature and median humidity.

    window_time is rendered as hh:mm only on the final projection, the window
    bound itself is kept as a timestamp throughout the computation.
    """
    return """
        SELECT
            DATE_FORMAT(TUMBLE_START(ts_event, INTERVAL '1' MINUTE), 'HH:mm') AS window_time,
            type_name AS type_name,
            AVG(temperature) AS avg_temperature,
            median(humidity) AS median_humidity
        FROM enriched
        WHERE type_name IS NOT NULL
        GROUP BY TUMBLE(ts_event, INTERVAL '1' MINUTE), type_name
    """


def build(t_env, iot_stream):
    """Wire the whole pipeline and return the insert statement (not executed)."""
    t_env.create_temporary_function("median", median)
    register_device_types_table(t_env)
    to_iot_table(t_env, iot_stream)
    create_enriched_view(t_env)
    create_sink_table(t_env)
    return "INSERT INTO iot_minute_agg " + aggregation_sql()
