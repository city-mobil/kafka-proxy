all: build

build: test
	cargo build

build_no_tests: build_release

build_release:
	cargo build --release

run_debug:
	target/debug/kprf --config=config_example.yaml

clean:
	cargo clean

test:
	cargo test --workspace && cargo test --package ratelimit --lib tests
