FROM maven:3.9-eclipse-temurin-21 as build

WORKDIR /app

COPY . .

RUN mvn package

FROM eclipse-temurin:21-jre-alpine

ENV JAVA_OPTS "-Xmx1g -Xms1g"

WORKDIR /app

COPY --from=build /app/target/global-search-1.0-SNAPSHOT-jar-with-dependencies.jar ./

VOLUME [ "/data" ]

CMD java $JAVA_OPTS -jar global-search-1.0-SNAPSHOT-jar-with-dependencies.jar
