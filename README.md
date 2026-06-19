# IoT streaming pipeline на Flink (PyFlink)

Сквозной потоковый pipeline в event time. Генератор раз в секунду публикует
IoT-события в Kafka, Flink-джоба читает их, обогащает справочником типов
устройств из Postgres, считает за каждую минуту среднюю температуру и медиану
влажности и пишет результат обратно в Kafka.

## Что внутри

```
[Генератор IoT] --json--> Kafka topic iot_events
                                  |
                                  v
                    KafkaSource (DataStream API)            чтение через DataStream
                                  |  parse + явная схема
                                  v
                    from_data_stream(...) -> Table          переход DataStream -> Table
                                  |  event time + watermark
   Postgres --JDBC connector--> device_types (Table)        source на Table/SQL API
                                  |
                                  v
                  LOOKUP JOIN по type_id = id                обогащение type_name
                                  |
                                  v
        TUMBLE(1 минута): AVG(temperature) + median(humidity) оконная агрегация
                                  |
                                  v
        Kafka sink table (connector kafka) + INSERT INTO     sink на Table/SQL API
                                  v
                         Kafka topic iot_minute_agg
```

Обязательные требования закрыты так:
- Source на Table/SQL API: справочник Postgres через JDBC connector.
- Sink на Table/SQL API: результат через Kafka sink table и INSERT INTO.
- Переход DataStream -> Table: Kafka читается через KafkaSource (DataStream),
  затем from_data_stream с явной схемой, назначением event-time колонки и watermark.

## Контракт

Входной топик iot_events, JSON:

```json
{"type_id": 1, "event_time": 1781875965000, "temperature": 18.32, "humidity": 44.27}
```

- type_id: 1..4, совпадает с id в справочнике device_types.
- event_time: epoch миллисекунды (event time).

Выходной топик iot_minute_agg, JSON:

```json
{"window_time": "13:35", "type_name": "thermostat", "avg_temperature": 24.55, "median_humidity": 49.58}
```

- window_time: начало минутного окна в формате hh:mm (UTC).
- type_name: имя типа устройства из Postgres.

Справочник Postgres: db testdb, таблица device_types(id INT, type_name VARCHAR),
наполнение в sql/dml.sql (id 1..4).

## Структура

```
.
├── docker-compose.yml      Kafka, Postgres, генератор, Flink-джоба
├── Dockerfile              образ с PyFlink, JDK 11, коннекторами
├── requirements.txt        apache-flink, kafka-python, psycopg2-binary
├── sql/
│   ├── ddl.sql             CREATE TABLE device_types
│   └── dml.sql             наполнение справочника (id 1..4)
├── generator/
│   └── iot_producer.py     публикация IoT-событий раз в секунду
├── flink_job/
│   ├── config.py           топики, bootstrap, jdbc url, пути к jars
│   ├── sources.py          KafkaSource (DataStream) + device_types (JDBC, SQL)
│   ├── pipeline.py         переход в Table, join, окно, агрегация, sink
│   ├── udf.py              кастомная агрегатная UDF для медианы
│   └── main.py             точка входа
└── sample_output.txt       пример выходных сообщений
```

## Заметки по реализации

- Медиана. Встроенной медианы или PERCENTILE в Flink 1.20 нет, поэтому медиана
  влажности считается кастомной агрегатной UDF (flink_job/udf.py): аккумулятор
  собирает значения окна, на финализации сортирует и берёт середину. Окно - одна
  минута, значений немного, держать их в памяти нормально.
- Event time и watermark. Колонка ts_event выводится из event_time через
  TO_TIMESTAMP_LTZ(event_time, 3), watermark задан с допустимой задержкой 5 секунд.
  Без watermark окна в event time не закрылись бы.
- Append-only sink. Прямая запись окон через Kafka sink table даёт append-only
  результат, поэтому формат json в Kafka подходит без changelog.
- window_time форматируется в hh:mm только на финальном SELECT, сама агрегация
  идёт в timestamp. Метка выводится в UTC (table.local-time-zone = UTC).
