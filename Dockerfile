FROM flink:2.0.0

USER root

# ============================================================================
# Install Python 3.11 and dependencies
# ============================================================================
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
        python3.11 python3.11-dev python3-pip \
        wget curl git build-essential vim && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Make python3.11 default
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 && \
    update-alternatives --set python3 /usr/bin/python3.11 && \
    ln -sf /usr/bin/python3 /usr/bin/python

# ============================================================================
# Install pip packages
# ============================================================================
RUN pip3 install --upgrade pip setuptools wheel

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt && \
    pip3 install --no-cache-dir \
        "numpy<1.25.0"


RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/4.0.0-2.0/flink-connector-kafka-4.0.0-2.0.jar \
         -P /opt/flink/lib && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.0.0/kafka-clients-4.0.0.jar \
         -P /opt/flink/lib
