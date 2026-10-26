# Apache Kafka 0.9.0.0 (Scala 2.11) on Java 8 — ZooKeeper와 Broker 모두 이 이미지로 실행
FROM eclipse-temurin:8-jre

ARG KAFKA_VERSION=0.9.0.0
ARG SCALA_VERSION=2.11

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && curl -fsSL "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" \
    | tar -xz -C /opt \
 && ln -s "/opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" /opt/kafka

COPY server.properties /opt/kafka/config/server.properties
COPY zookeeper.properties /opt/kafka/config/zookeeper.properties

ENV PATH="/opt/kafka/bin:${PATH}"
WORKDIR /opt/kafka
