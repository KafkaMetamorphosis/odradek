SERVICES := kafka

.PHONY: deps stop-deps wait-for-kafka create-test-topic unit integration test run

deps:
	@all_running=true; \
	for svc in $(SERVICES); do \
		status=$$(docker-compose ps --status running --services 2>/dev/null | grep -x "$$svc"); \
		if [ -z "$$status" ]; then \
			all_running=false; \
			break; \
		fi; \
	done; \
	if [ "$$all_running" = true ]; then \
		echo "Dependencies already running"; \
	else \
		echo "Starting dependencies..."; \
		docker-compose up -d; \
	fi

stop-deps:
	docker-compose down

wait-for-kafka: deps
	@echo "Waiting for Kafka..."; \
	until docker-compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do \
		sleep 2; \
	done; \
	echo "Kafka ready."

create-test-topic: wait-for-kafka
	docker-compose exec -T kafka kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create --if-not-exists \
		--topic ODRADEK-TEST-TOPIC \
		--partitions 1 \
		--replication-factor 1

unit:
	lein test :odradek.unit

integration: create-test-topic
	lein test :odradek.integration

test: unit integration

run: wait-for-kafka
	lein run
