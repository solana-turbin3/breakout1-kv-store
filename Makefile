.PHONY: build run test check clean fmt lint bench

build:
	cargo build

run:
	cargo run

test:
	cargo test

check:
	cargo clippy 

clean:
	cargo clean

fmt:
	cargo +nightly fmt --all

bench:
	cargo bench --bench engine_bench
