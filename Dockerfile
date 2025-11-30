FROM arm64v8/flink:2.0.0
USER root
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
        python3.11 \
        openjdk-17-jdk-headless \
        python3.11-dev \
        python3-pip \
        wget \
        curl \
        git \
        build-essential \
        vim && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 && \
    update-alternatives --set python3 /usr/bin/python3.11

RUN ldconfig /usr/lib && \
    ln -s /usr/bin/python3 /usr/bin/python

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:$PATH"

# =============================================================================
RUN pip3 install --upgrade pip setuptools wheel
COPY requirements.txt /tmp/requirements.txt

RUN pip3 install --no-cache-dir -r /tmp/requirements.txt && \
    pip3 install --no-cache-dir \
        git+https://github.com/thinh-vu/vnstock \
        "numpy<1.25.0"

# =============================================================================
# Download Flink Connector JARs
# =============================================================================

RUN cp -r /usr/local/lib/python3.11/dist-packages/pyflink/* /opt/flink/

RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/4.0.0-2.0/flink-connector-kafka-4.0.0-2.0.jar \
         -P /opt/flink/opt && \
    # wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.0-2.0/flink-sql-connector-kafka-4.0.0-2.0.jar \
    #      -P /opt/flink/lib && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.0.0/kafka-clients-4.0.0.jar \
         -P /opt/flink/opt 