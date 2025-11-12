FROM python:3.11-slim

RUN apt-get update -qq && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless \
        wget \
        git \
        build-essential \
        vim && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app

COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    pip install git+https://github.com/thinh-vu/vnstock && \

RUN mkdir -p /usr/local/lib/pyflink/opt && \
    wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/4.0.0-2.0/flink-connector-kafka-4.0.0-2.0.jar -P /usr/local/lib/pyflink/opt && \
    wget -q https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/4.0.0-2.0/flink-sql-connector-kafka-4.0.0-2.0.jar -P /usr/local/lib/pyflink/opt && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.0.0/kafka-clients-4.0.0.jar -P /usr/local/lib/pyflink/opt

COPY . .

CMD ["python3", "main.py"]