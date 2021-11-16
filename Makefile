VERSION := `git describe --tags --dirty --always 2>/dev/null || echo no_tag`
COMMIT := `git show --pretty=format:"%h" --no-patch 2>/dev/null || echo no_commit`
ENVS := APP_VERSION=${VERSION} GIT_COMMIT=${COMMIT}

all: build

build: build_release

build_with_tests: test build_release

build_release:
	${ENVS} cargo build --release

build_debug:
	${ENVS} cargo build

run_debug:
	target/debug/kprf --config=config_example.yaml

run:
	target/release/kprf --config=config_example.yaml

clean:
	cargo clean

test:
	${ENVS} RUST_BACKTRACE=full cargo test --workspace && cargo test --package ratelimit --lib tests
