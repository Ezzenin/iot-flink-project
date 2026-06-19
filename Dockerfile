# Image that runs both the IoT generator and the PyFlink job.
# PyFlink does not install in a venv on Apple Silicon or Windows, so the job
# runs inside this Linux image where all wheels are available. It also needs a
# JVM for the embedded Flink minicluster, so Temurin 11 is installed.
FROM python:3.11-slim-bookworm

ARG KAFKA_CONNECTOR=3.3.0-1.20
ARG JDBC_CONNECTOR=3.3.0-1.20
ARG PG_DRIVER=42.7.7

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# JVM (Temurin 11 JDK) for the Flink minicluster and to build pemja, plus a
# compiler since pemja has no prebuilt wheel for linux/arm64. The full JDK
# (not just the JRE) is needed because pemja compiles against the JNI headers.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget curl ca-certificates gnupg build-essential && \
    mkdir -p /etc/apt/keyrings && \
    wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public \
        | gpg --dearmor -o /etc/apt/keyrings/adoptium.gpg && \
    echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb bookworm main" \
        > /etc/apt/sources.list.d/adoptium.list && \
    apt-get update && \
    apt-get install -y --no-install-recommends temurin-11-jdk && \
    ln -s "$(dirname "$(dirname "$(readlink -f "$(which javac)")")")" /opt/java && \
    rm -rf /var/lib/apt/lists/*

# JAVA_HOME is consumed both by the pemja build below and by the Flink
# minicluster at runtime. /opt/java is an arch-independent symlink to the JDK.
ENV JAVA_HOME=/opt/java
ENV PATH=/opt/java/bin:$PATH

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Connector jars: Kafka SQL connector, JDBC connector and the Postgres driver.
RUN mkdir -p /app/jars && \
    curl -fsSL -o /app/jars/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR}/flink-sql-connector-kafka-${KAFKA_CONNECTOR}.jar && \
    curl -fsSL -o /app/jars/flink-connector-jdbc-${JDBC_CONNECTOR}.jar \
        https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/${JDBC_CONNECTOR}/flink-connector-jdbc-${JDBC_CONNECTOR}.jar && \
    curl -fsSL -o /app/jars/postgresql-${PG_DRIVER}.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/${PG_DRIVER}/postgresql-${PG_DRIVER}.jar

COPY flink_job/ ./flink_job/
COPY generator/ ./generator/

ENV JARS_DIR=/app/jars
ENV PYTHONPATH=/app/flink_job

CMD ["python", "flink_job/main.py"]
