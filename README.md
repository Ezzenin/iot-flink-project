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

## Почему всё в Docker

PyFlink не ставится в локальный venv на Apple Silicon и на Windows: у apache-beam
нет arm64-колёс ниже версии 2.70, а Flink требует beam <= 2.61, под Windows у
PyFlink колёс нет вовсе. Внутри Linux-контейнера все колёса доступны и для
x86_64, и для arm64, поэтому генератор и джоба запускаются в контейнерах.
Нативное расширение pemja (нужно для Python UDF медианы) под arm64 собирается из
исходников прямо в образе, для чего в него ставится JDK 11 и компилятор.

## Требования

- Docker и Docker Compose.
- Доступ в интернет при первой сборке (тянутся образы, зависимости и jar-коннекторы).

## Как запустить

Из корня репозитория:

```bash
docker compose up --build
```

Эта команда:
1. поднимает Kafka и Postgres, дожидается их готовности;
2. создаёт топики iot_events и iot_minute_agg (сервис kafka-init);
3. применяет sql/ddl.sql и sql/dml.sql к Postgres при инициализации;
4. собирает образ с PyFlink и коннекторами;
5. запускает генератор и Flink-джобу.

Первая сборка идёт несколько минут (скачивается apache-flink и собирается pemja).

## Где смотреть результат

Выходной топик читается консольным консьюмером:

```bash
docker exec iot_kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic iot_minute_agg --from-beginning
```

Входной поток можно посмотреть так же по топику iot_events. Каждое закрытое
минутное окно даёт по одному сообщению на тип устройства. Первое окно после
старта джобы может быть неполным, так как джоба подключается к топику с latest
offset в середине минуты; полные окна считаются корректно.

Пример вывода лежит в [sample_output.txt](sample_output.txt).

## Остановка

```bash
docker compose down
```

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
