SERVICES := kafka

.PHONY: run-deps stop-deps create-test-topic unit integration test run

run-deps:
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

unit:
	lein test :odradek.unit

integration: run-deps
	lein test :odradek.integration

test: unit integration

run: run-deps
	lein run
