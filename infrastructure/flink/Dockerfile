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
    && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    pip3 install apache-flink==1.20.0 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER flink
