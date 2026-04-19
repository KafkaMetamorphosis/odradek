.PHONY: run-deps stop-deps unit integration test run seed

run-deps:
	docker-compose -f docker-compose.local.yml up -d

stop-deps:
	docker-compose -f docker-compose.local.yml down

unit:
	lein test :odradek.unit

integration:
	docker-compose -f docker-compose.test.yaml up -d
	lein test :odradek.integration
	docker-compose -f docker-compose.test.yaml down

test: unit integration

run: run-deps
	lein run

seed: run-deps
	lein with-profile +dev run -m odradek.seed
