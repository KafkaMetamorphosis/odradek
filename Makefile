.PHONY: run-deps stop-deps unit integration test run seed

LOCAL_SERVICES := kafka-1 kafka-2 kafka-3

run-deps:
	@all_running=true; \
	for svc in $(LOCAL_SERVICES); do \
		status=$$(docker-compose -f docker-compose.local.yml ps --status running --services 2>/dev/null | grep -x "$$svc"); \
		if [ -z "$$status" ]; then \
			all_running=false; \
			break; \
		fi; \
	done; \
	if [ "$$all_running" = true ]; then \
		echo "Local dependencies already running"; \
	else \
		echo "Starting local dependencies..."; \
		docker-compose -f docker-compose.local.yml up -d; \
	fi

stop-deps:
	docker-compose -f docker-compose.local.yml down

unit:
	lein test :odradek.unit

integration:
	@status=$$(docker-compose -f docker-compose.test.yaml ps --status running --services 2>/dev/null | grep -x "kafka"); \
	if [ -n "$$status" ]; then \
		echo "Test dependencies already running"; \
		already_running=true; \
	else \
		echo "Starting test dependencies..."; \
		docker-compose -f docker-compose.test.yaml up -d; \
		already_running=false; \
	fi; \
	lein test :odradek.integration; \
	test_exit=$$?; \
	if [ "$$already_running" = false ]; then \
		docker-compose -f docker-compose.test.yaml down; \
	fi; \
	exit $$test_exit

test: unit integration

run: run-deps
	lein run

seed: run-deps
	lein with-profile +dev run -m odradek.seed
