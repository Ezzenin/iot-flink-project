"""Job entry point.

Builds the StreamExecutionEnvironment and StreamTableEnvironment, registers the
connector jars, constructs the pipeline and submits it.
"""

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

import config
from sources import build_kafka_stream
from pipeline import build


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    jars = config.jar_paths()
    if jars:
        env.add_jars(*jars)

    settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    # Render the hh:mm window label deterministically in UTC.
    t_env.get_config().set("table.local-time-zone", "UTC")

    iot_stream = build_kafka_stream(env)
    insert_sql = build(t_env, iot_stream)

    t_env.execute_sql(insert_sql).wait()


if __name__ == "__main__":
    main()
