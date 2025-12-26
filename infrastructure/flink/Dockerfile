# ===========================================
# Custom Flink Image with Python Support
# ===========================================
FROM flink:1.20-java17

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-dev \
        python3-pip \
        python3-venv \
        wget \
    && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    pip3 install apache-flink==1.20.0 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download SQL connectors for Flink SQL Client
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar && \
    wget -P /opt/flink/lib/ https://github.com/knaufk/flink-faker/releases/download/v0.5.3/flink-faker-0.5.3.jar

USER flink
