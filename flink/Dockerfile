FROM flink:2.0.0

USER root

# Công cụ cơ bản + Python (cần cho PyFlink: chạy job.py và thực thi Python UDF)
RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
        wget curl vim netcat-openbsd \
        python3 python3-pip python3-dev build-essential && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# PyFlink (khớp phiên bản Flink) để submit Table API job + chạy Python UDF.
RUN pip3 install --no-cache-dir apache-flink==2.0.0

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /opt/flink

# Connector Kafka + JSON format cho Flink SQL
RUN wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.0-2.0/flink-sql-connector-kafka-4.0.0-2.0.jar \
         -P /opt/flink/lib && \
    wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-json/2.0.0/flink-json-2.0.0.jar \
         -P /opt/flink/lib
