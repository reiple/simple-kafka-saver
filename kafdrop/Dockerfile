# Kafdrop (HomeAdvisor 원본) — Kafka 0.9 브로커를 볼 수 있는 웹 UI
# ec7324c(2019-12)부터 kafka-clients 2.x로 바뀌어 0.9와 연결되지 않으므로,
# ZooKeeper + Kafka 0.8.2 SimpleConsumer를 쓰던 마지막 커밋으로 빌드한다.
FROM maven:3.9-eclipse-temurin-8 AS build
ARG KAFDROP_COMMIT=b0c55bf12f102aab8e83801200b25ab41c6ef77c
WORKDIR /build
RUN curl -fsSL "https://github.com/HomeAdvisor/Kafdrop/archive/${KAFDROP_COMMIT}.tar.gz" \
    | tar -xz --strip-components=1
# 최신 Maven은 http 저장소를 막으므로 https로 변경
RUN sed -i 's#http://repo1.maven.org#https://repo1.maven.org#; s#http://packages.confluent.io#https://packages.confluent.io#' pom.xml \
 && mvn -B -q -DskipTests package \
 && cp target/kafdrop-*.jar /build/kafdrop.jar

FROM eclipse-temurin:8-jre
WORKDIR /app
COPY --from=build /build/kafdrop.jar /app/kafdrop.jar
EXPOSE 9000
ENTRYPOINT ["java", "-jar", "/app/kafdrop.jar"]
