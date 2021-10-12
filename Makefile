VERSION := `git describe --tags --dirty --always 2>/dev/null || echo no_tag`
COMMIT := `git show --pretty=format:"%h" --no-patch 2>/dev/null || echo no_commit`

all: build

build:
	APP_VERSION=${VERSION} GIT_COMMIT=${COMMIT} cargo build

build_release:
	cargo build --release

run_debug:
	target/debug/kprf --config=config_example.yaml

clean:
	cargo clean
