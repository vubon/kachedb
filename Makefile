.PHONY: all build release test bench server cli python-test clean fmt check

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

python-test:
	PYTHONPATH=bindings/python python3 bindings/python/tests/test_client.py

fmt:
	cargo fmt --all

check:
	cargo check --workspace

clean:
	cargo clean
