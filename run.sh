#!/bin/bash

mvn clean install

export QUARKUS_CONFIG_LOCATIONS=config.properties

java -jar server/target/quarkus-app/quarkus-run.jar