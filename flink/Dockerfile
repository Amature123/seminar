FROM flink:2.0.0

USER root
RUN apt-get update -qq &&\       
    apt-get install -y --no-install-recommends\
        wget curl vim netcat &&\ 
    apt-get clean &&\
    rm -rf /var/lib/apt/lists/*

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set working directory
WORKDIR /opt/flink

RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.0-2.0/flink-sql-connector-kafka-4.0.0-2.0.jar \
         -P /opt/flink/lib &&\
    wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-json/2.0.0/flink-json-2.0.0.jar \
         -P /opt/flink/lib