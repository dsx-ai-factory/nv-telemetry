#
# Build and test everything.
#
# Lint levels live in the workspace manifest rather than in the flags below, so
# an editor reports exactly what this pipeline will. The only thing added here
# is promoting warnings to errors, which is wanted in CI but not while editing.
#
# rustfmt.toml uses a nightly-only option, and stable rustfmt would ignore it
# with a warning rather than an error. Every format step therefore names the
# toolchain, so a plain `cargo fmt` cannot quietly disagree with the pipeline.
#
# The schema is checked by buf, configured in buf.yaml. buf is a checker only:
# the descriptor set is built by protox from schema/build.rs, so `cargo build`
# needs no external binary and a consumer of the data plane pulls only prost.
#

export RUSTDOCFLAGS := -D warnings

fmt-toolchain := nightly-2026-06-16

buf ?= buf

# Pinned so that `make fmt` locally and the format check in CI agree.
# Set empty to disable the check.
buf-version ?= 1.72.0

# Several targets share a name with a directory in the workspace, `codegen`
# being the one that matters. Without this, make sees the directory, decides
# the target is up to date, and silently does nothing.
.PHONY: all ci fmt bench codegen check-codegen proto-lint \
	check-proto-format check-redfish-features require-buf \
	rust-install clean test-bmc-mock

# `--locked` throughout: generated output is byte-compared, and its bytes are
# decided by prost-build and prettyplease. Manifests state semver ranges, and
# a formatter is free to change its output in a patch release, so the lockfile
# is the only thing that holds the generated tree to one rendering. Without it
# held authoritative, a patch bump rewrites the generated tree and surfaces as
# "generated files differ" on a pull request that touched neither.
#
# The cost is that we otherwise only ever build one resolution. The scheduled
# latest-dependencies job clears this variable to resolve freshly, which is
# where upstream breakage and generator-output drift are meant to be found.
cargo-locked ?= --locked

define build-and-test
	cargo +$(fmt-toolchain) fmt --all -- --check
	+$(MAKE) check-proto-format
	+$(MAKE) proto-lint
	+$(MAKE) check-codegen
	+$(MAKE) check-redfish-features
	cargo clippy $(cargo-locked) --workspace --all-targets -- -D warnings
	cargo clippy $(cargo-locked) --workspace --all-targets $1 -- -D warnings
	cargo build $(cargo-locked) --workspace
	cargo build $(cargo-locked) --workspace $1
	cargo test $(cargo-locked) --workspace -- --no-capture
	cargo test $(cargo-locked) --workspace $1 -- --no-capture
	cargo doc $(cargo-locked) --workspace --no-deps $1

endef


all:
	$(call build-and-test,--all-features)

ci: rust-install
	$(call build-and-test,--all-features)

fmt: require-buf
	cargo +$(fmt-toolchain) fmt --all
	$(buf) format -w

# Style and consistency rules over the schema. These cover what our own
# compiler deliberately does not: naming, package/directory correspondence,
# and enum value prefixes, which matter because proto enum values are scoped to
# the enclosing package rather than to their enum.
proto-lint: require-buf
	$(buf) lint

check-proto-format: require-buf
	$(buf) format --diff --exit-code

require-buf:
	@BUF=$(buf) bash tools/require-buf.sh $(buf-version)

# Generated output is checked in, so a schema change and its generated output
# land in the same reviewable diff and no downstream build needs a protobuf
# compiler. `check-codegen` runs inside the pipeline above, ahead of the build
# steps, so a schema edit without regeneration fails immediately rather than
# surfacing later as a confusing compile error.
codegen:
	cargo run $(cargo-locked) -p nv-telemetry-codegen -- generate

check-codegen:
	cargo run $(cargo-locked) -p nv-telemetry-codegen -- --check

# Optional cross-repository HTTP test; first build the sibling bmc-mock and
# point BMC_MOCK_ROOT at its target directory.
test-bmc-mock:
	cargo test $(cargo-locked) -p nv-telemetry-probe --test e2e_http -- --ignored --nocapture

# Every transport row the Redfish crate supports is explicit here. Workspace
# default/all-feature builds cover only the HTTP and combined rows and
# previously allowed an isolated feature to decay behind dev-dependency
# feature unification.
check-redfish-features:
	cargo clippy $(cargo-locked) -p nv-telemetry-redfish --no-default-features --lib -- -D warnings
	cargo clippy $(cargo-locked) -p nv-telemetry-redfish --no-default-features --features bmc-mock --lib -- -D warnings
	cargo clippy $(cargo-locked) -p nv-telemetry-redfish --no-default-features --features bmc-http --lib -- -D warnings
	cargo clippy $(cargo-locked) -p nv-telemetry-redfish --no-default-features --features bmc-http,bmc-mock --lib -- -D warnings
	cargo test $(cargo-locked) -p nv-telemetry-redfish --no-default-features --lib
	cargo test $(cargo-locked) -p nv-telemetry-redfish --no-default-features --features bmc-mock
	cargo test $(cargo-locked) -p nv-telemetry-redfish --no-default-features --features bmc-http --lib
	cargo test $(cargo-locked) -p nv-telemetry-redfish --no-default-features --features bmc-http,bmc-mock

# Instruction counts under Valgrind, so results do not depend on machine
# load. Needs valgrind and a gungraun-runner matching the gungraun version
# the lockfile resolved. CI compares these against the merge-base; locally
# they are absolute numbers unless a baseline was saved.
bench:
	cargo bench --workspace --all-features

rust-install:
	rustup component add clippy rustfmt
	rustup toolchain install $(fmt-toolchain) --profile minimal --component rustfmt

clean:
	rm -rf target
