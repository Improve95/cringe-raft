FROM eclipse-temurin:25-jdk AS build

WORKDIR /workspace

COPY gradle/ gradle/
COPY --chmod=0755 gradlew gradlew
COPY build.gradle settings.gradle ./
COPY src/ src/

RUN --mount=type=cache,target=/root/.gradle ./gradlew --no-daemon bootJar

FROM eclipse-temurin:25-jre

WORKDIR /app

RUN groupadd --system raft && useradd --system --gid raft --home-dir /app raft

COPY --from=build --chown=raft:raft /workspace/build/libs/*.jar /app/raft.jar

USER raft:raft

ENV SERVER_PORT=8080
EXPOSE 8080

ENTRYPOINT ["java", "-jar", "/app/raft.jar"]
