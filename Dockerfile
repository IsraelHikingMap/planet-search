FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /app

COPY . .

RUN mvn clean package dependency:copy-dependencies

FROM eclipse-temurin:21-jre-alpine

ENV JAVA_OPTS="-Xmx2g -Xms2g"

WORKDIR /app

COPY --from=build /app/target/ ./

VOLUME [ "/data" ]

ENTRYPOINT java $JAVA_OPTS -cp "classes:dependency/*" il.org.osm.israelhiking.MainClass "$0" "$@"
