#!/bin/sh
docker-compose exec kafka kafka-console-consumer --topic=platform.sources.event-stream --bootstrap-server=localhost:29092
