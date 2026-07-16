FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /workspace
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src ./src
RUN mvn -B clean verify

FROM flink:1.20.2-scala_2.12-java17
COPY --from=build /workspace/target/ai-data-pipeline-1.0.0.jar /opt/flink/usrlib/ai-data-pipeline.jar
COPY --from=build /workspace/target/runtime-libs/*.jar /opt/flink/lib/

USER flink
