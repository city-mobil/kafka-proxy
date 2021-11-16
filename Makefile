VERSION := `git describe --tags --dirty --always 2>/dev/null || echo no_tag`
COMMIT := `git show --pretty=format:"%h" --no-patch 2>/dev/null || echo no_commit`

all: build

build: build_release

build_with_tests: test build_release

build_release:
	APP_VERSION=${VERSION} GIT_COMMIT=${COMMIT} cargo build --release

run_debug:
	target/debug/kprf --config=config_example.yaml

clean:
	cargo clean

test:
	cargo test --workspace && cargo test --package ratelimit --lib tests
