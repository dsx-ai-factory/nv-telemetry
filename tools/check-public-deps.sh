#!/usr/bin/env bash
# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Fails when a published crate's public API exposes an nv-redfish type.
#
# An nv-redfish release must never force an nv-telemetry one, so nv-redfish
# stays a private dependency of every crate on crates.io. Under
# `-Zpublic-dependency` every dependency not marked `public` is private, and
# the compiler names each one a public signature exposes; only nv-redfish
# leaks fail here. Unpublished crates (the code generator, the probe) are
# exempt: nothing downstream builds against them.
set -euo pipefail

toolchain=${1:?usage: check-public-deps.sh <nightly toolchain>}

# Assigned before use, so a metadata or jq failure trips set -e.
packages=$(cargo metadata --format-version=1 --no-deps \
  | jq -r '.packages[] | select(.publish != []) | "--package=\(.name)"')
if [ -z "$packages" ]; then
  echo "no published packages found" >&2
  exit 1
fi

# shellcheck disable=SC2086 # one argument per package
diagnostics=$(cargo "+$toolchain" -Zpublic-dependency check --locked --all-features \
  --lib $packages --target-dir target/public-dependency --message-format=json)
leaks=$(jq -r 'select(.reason == "compiler-message")
  | select(.message.code.code == "exported_private_dependencies")
  | select(.message.message | test("private dependency .nv_redfish"))
  | .message.rendered' <<< "$diagnostics")

if [ -n "$leaks" ]; then
  echo "$leaks"
  echo "a published crate exposes nv-redfish in its public API" >&2
  exit 1
fi
echo "no nv-redfish type in a published crate's public API"
