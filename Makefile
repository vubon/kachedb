.PHONY: all build release test bench bench-live bench-live-set bench-live-get server cli python-test clean fmt check

all: build test

build:
	cargo build --workspace

release:
	cargo build --workspace --release

test:
	cargo test --workspace

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

python-test:
	PYTHONPATH=bindings/python python3 bindings/python/tests/test_client.py

fmt:
	cargo fmt --all

check:
	cargo check --workspace

clean:
	cargo clean
