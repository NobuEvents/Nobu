# Start with a base image containing Java runtime and Maven
FROM maven:3.8.4-openjdk-17-slim AS build

# The application's .jar file will reside in /usr/src/app
WORKDIR /usr/src/source

# Copy the pom.xml file first, for separate dependency resolution
COPY ./pom.xml ./pom.xml

# Copy your source code in the container
COPY ./server ./server
COPY ./spec ./spec
COPY ./connectors ./connectors



# Clean and package the application with Maven, this will also download dependencies
RUN mvn clean install package -DskipTests

# Start with a base image containing Java runtime
FROM openjdk:17-slim

WORKDIR /app

# Include application's .jar files in container
COPY --from=build /usr/src/source/server/target/quarkus-app /app/quarkus-app

# Copy the configuration files
COPY ./server/src/main/resources/route.yaml /app/quarkus-app/server/src/main/resources/route.yaml
COPY ./server/src/main/resources/schema /app/quarkus-app/server/src/main/resources/schema

COPY config.properties /app/quarkus-app/config.properties

WORKDIR /app/quarkus-app


ENV QUARKUS_CONFIG_LOCATIONS=config.properties

EXPOSE 8080


CMD java -jar quarkus-run.jar -Dquarkus.http.port=8080
