FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /app

COPY . .

RUN mvn package

FROM eclipse-temurin:21-jre-alpine

ENV JAVA_OPTS="-Xmx2g -Xms2g"

WORKDIR /app

COPY --from=build /app/target/ ./

VOLUME [ "/data" ]

ENTRYPOINT java $JAVA_OPTS -jar planet-search.jar -cp ./ "$0" "$@"
