all: build

build:
	cargo build

build_release:
	cargo build --release

run_debug:
	target/debug/kprf --config=config_example.yaml

clean:
	cargo clean
