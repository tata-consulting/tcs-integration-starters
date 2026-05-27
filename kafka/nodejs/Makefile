.PHONY: start stop create-topics test build clean

start:
	docker compose up -d
	@echo "Waiting for Kafka and Schema Registry to be ready..."
	@docker compose exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1 || sleep 10

stop:
	docker compose down

create-topics:
	docker compose exec kafka kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic orders.created \
		--partitions 3 \
		--replication-factor 1
	docker compose exec kafka kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic orders.created.DLQ \
		--partitions 1 \
		--replication-factor 1

test:
	npm test

build:
	npx tsc --noEmit

clean:
	docker compose down -v
	rm -rf node_modules dist
