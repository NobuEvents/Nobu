#stage 1
FROM maven:3.9.1 as builder

WORKDIR /app
ADD pom.xml .
ADD server/pom.xml ./server/pom.xml
ADD spec/pom.xml ./spec/pom.xml

ADD connectors/pom.xml ./connectors/pom.xml
ADD connectors/kafka/pom.xml ./connectors/kafka/pom.xml
ADD connectors/console/pom.xml ./connectors/console/pom.xml

RUN mvn -pl spec verify --fail-never
ADD spec ./spec
RUN mvn -pl spec install

RUN mvn install

RUN mvn -pl connectors/kafka verify --fail-never #run tests
ADD connectors/kafka ./connectors/kafka
RUN mvn -pl connectors/kafka install

RUN mvn -pl connectors/console verify --fail-never
ADD connectors/console ./connectors/console
RUN mvn -pl connectors/console install

RUN mvn -pl connectors install

RUN mvn -pl server verify --fail-never
ADD server ./server
RUN mvn -pl server install

RUN mvn -pl spec,connectors,server package

#stage 2
From openjdk:17
WORKDIR /server


COPY --from=builder /app/server/target/quarkus-app/ .
EXPOSE 7070
CMD ["java", "-jar", "quarkus-run.jar"]
