docker-compose exec -it kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
docker-compose exec -it kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic transactions