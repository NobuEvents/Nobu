FROM maven:3.8.4-openjdk-17-slim AS build

WORKDIR /usr/src/source

# Build the application

COPY ./pom.xml ./pom.xml
COPY ./server ./server
COPY ./spec ./spec
COPY ./connectors ./connectors
RUN mvn clean install package -DskipTests

FROM openjdk:17-slim

WORKDIR /app

# Include Quarkus application's .jar files in container
COPY --from=build /usr/src/source/server/target/quarkus-app /app/quarkus-app

# Copy the configuration files
COPY ./server/src/main/resources/route.yaml /app/quarkus-app/server/src/main/resources/route.yaml
COPY ./server/src/main/resources/schema /app/quarkus-app/server/src/main/resources/schema

COPY config.properties /app/quarkus-app/config.properties

WORKDIR /app/quarkus-app


# Make sure set the Quarkus configuration file location
ENV QUARKUS_CONFIG_LOCATIONS=config.properties

EXPOSE 8080


CMD java -jar quarkus-run.jar -Dquarkus.http.port=8080
