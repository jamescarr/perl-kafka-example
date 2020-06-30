build:
	docker build -t perl-kafka-client .

run:
	docker run --network perl-kafka_default -e BROKER_SERVERS=broker perl-kafka-client
