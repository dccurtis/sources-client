#!/bin/sh
docker-compose exec kafka kafka-console-producer --topic=platform.sources.event-stream --broker-list=localhost:29092
