.PHONY: all build release test bench bench-live bench-live-set bench-live-get benchmark-reproduce server cli python-test clean fmt check lint docs docs-serve

all: build test

build:
	cargo build --workspace

release:
	cargo build --workspace --release

test:
	cargo test --workspace

lint:
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets -- -D warnings

bench:
	cargo bench --workspace

server: release
	./target/release/kachedb-server --port 6379

cli: release
	./target/release/kachedb-cli --port 6379

bench-live: release
	./target/release/kachedb-bench --port 6379 --requests 100000 --clients 50 --pipeline 16 --command PING

bench-live-set: release
	./target/release/kachedb-bench --port 6379 --requests 100000 --clients 50 --pipeline 16 --command SET

bench-live-get: release
	./target/release/kachedb-bench --port 6379 --requests 100000 --clients 50 --pipeline 16 --command GET

benchmark-reproduce:
	docker compose -f docker/docker-compose.yml up -d --build
	@sleep 2
	docker exec docker-kachedb-1 kachedb-bench -p 6379 -n 100000 -c 50 --pipeline 16 --command PING
	docker exec docker-kachedb-1 kachedb-bench -p 6379 -n 100000 -c 50 --pipeline 16 --command SET
	docker exec docker-kachedb-1 kachedb-bench -p 6379 -n 100000 -c 50 --pipeline 16 --command GET
	docker compose -f docker/docker-compose.yml down

benchmark-compare:
	./docker/run_benchmark.sh

docs:
	@if command -v mdbook >/dev/null 2>&1; then \
		mdbook build; \
	else \
		docker run --rm -v "$$(pwd):/book" -w /book peaceiris/mdbook:v0.4.40 build; \
	fi

docs-serve:
	@if command -v mdbook >/dev/null 2>&1; then \
		mdbook serve --open; \
	else \
		echo "Starting mdBook local server via Docker on http://localhost:3000 ..."; \
		docker run --rm -it --init -v "$$(pwd):/book" -w /book -p 3000:3000 peaceiris/mdbook:v0.4.40 serve --hostname 0.0.0.0; \
	fi

python-test:
	PYTHONPATH=bindings/python python3 bindings/python/tests/test_client.py

coverage:
	cargo llvm-cov --workspace --summary-only

coverage-html:
	cargo llvm-cov --workspace --open

fmt:
	cargo fmt --all

check:
	cargo check --workspace

clean:
	cargo clean
