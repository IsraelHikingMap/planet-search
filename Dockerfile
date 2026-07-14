FROM maven:3.9-eclipse-temurin-21 AS build

ARG VERSION=1.0-dev

WORKDIR /app

COPY . .

RUN mvn clean package dependency:copy-dependencies -Dbuild.version=$VERSION

FROM eclipse-temurin:21-jre-alpine

ARG VERSION=1.0-dev

LABEL org.opencontainers.image.title="planet-search"
LABEL org.opencontainers.image.source="https://github.com/IsraelHikingMap/planet-search"
LABEL org.opencontainers.image.version="$VERSION"

ENV JAVA_OPTS="-Xmx2g -Xms2g"

WORKDIR /app

COPY --from=build /app/target/ ./

VOLUME [ "/data" ]

ENTRYPOINT java $JAVA_OPTS -cp "classes:dependency/*" il.org.osm.israelhiking.MainClass "$0" "$@"
