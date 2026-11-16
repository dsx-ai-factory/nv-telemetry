#!/usr/bin/env node
// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// Checks every instance of the specs in spec/ against what `checks` below
// says it must hold and must fail, then draws the traces that tests replay
// against the code: orchestration/tests/spec_replay.rs against the
// collector and sources/redfish/tests/event_stream_replay.rs against the event
// stream.
//
// Needs Node, and for the checks a Java newer than 11. QUINT overrides how
// Quint is started; `traces` as the one argument skips the checks.

import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const quint = (process.env.QUINT ?? "npx --yes @informalsystems/quint@0.33.1").split(" ");

const ENDPOINT = ["pollServed", "debtBounded", "streamReturns", "pollsNotSilenced", "pollsDoNotOverlap"];
const STREAM = ["stopsOnlyWhenTold", "lossesReported", "keysNameOneEvent"];
const LIFECYCLE = ["retirementEnds", "retiredIsLast", "retiresOnlyWhenTold"];
const LOGWALK = ["nothingSkipped", "shipsOnce", "keepsNewest", "cutsOnlyWhenShort"];
const COLLECTOR = ["inOrder", "oneLifeAtATime"];

// Each instance holds its spec's invariants except those it must fail.
const checks = [
  { spec: "endpoint", invariants: ENDPOINT, main: "steady" },
  { spec: "endpoint", invariants: ENDPOINT, main: "two_each" },
  { spec: "endpoint", invariants: ENDPOINT, main: "cheap_streams" },
  { spec: "endpoint", invariants: ENDPOINT, main: "cheap_streams_before_floor", fails: ["pollServed", "pollsNotSilenced"] },
  { spec: "endpoint", invariants: ENDPOINT, main: "hostile" },
  { spec: "endpoint", invariants: ENDPOINT, main: "hung_connect_before_deadline", fails: ["streamReturns"] },
  { spec: "endpoint", invariants: ENDPOINT, main: "hung_connect_before_apart", fails: ["pollServed", "streamReturns", "pollsNotSilenced"] },
  { spec: "endpoint", invariants: ENDPOINT, main: "two_failing_streams" },
  { spec: "endpoint", invariants: ENDPOINT, main: "two_failing_streams_before_apart", fails: ["pollsNotSilenced"] },
  // Too large to check in full.
  { spec: "endpoint", invariants: ENDPOINT, main: "two_each_hostile", simulated: true },
  { spec: "stream", invariants: STREAM, main: "mock" },
  { spec: "stream", invariants: STREAM, main: "bmcweb" },
  { spec: "stream", invariants: STREAM, main: "mock_stale_position" },
  { spec: "stream", invariants: STREAM, main: "mock_before_reopen", fails: ["stopsOnlyWhenTold"] },
  { spec: "stream", invariants: STREAM, main: "bmcweb_before_new_run", fails: ["keysNameOneEvent"] },
  { spec: "stream", invariants: STREAM, main: "bmcweb_stale_position", fails: ["keysNameOneEvent"] },
  { spec: "lifecycle", invariants: LIFECYCLE, main: "with_timeout" },
  { spec: "lifecycle", invariants: LIFECYCLE, main: "stream_only_without_timeout" },
  { spec: "lifecycle", invariants: LIFECYCLE, main: "without_timeout", fails: ["retirementEnds"] },
  { spec: "logwalk", invariants: LOGWALK, main: "plain" },
  { spec: "logwalk", invariants: LOGWALK, main: "newest_first" },
  { spec: "logwalk", invariants: LOGWALK, main: "filtering" },
  { spec: "logwalk", invariants: LOGWALK, main: "refilled" },
  { spec: "logwalk", invariants: LOGWALK, main: "refilled_same_second", fails: ["nothingSkipped"] },
  { spec: "logwalk", invariants: LOGWALK, main: "stepping_back" },
  { spec: "logwalk", invariants: LOGWALK, main: "stepping_back_newest_first" },
  { spec: "logwalk", invariants: LOGWALK, main: "stepping_back_before_ids_outrank", fails: ["nothingSkipped", "shipsOnce"] },
  { spec: "logwalk", invariants: LOGWALK, main: "filtering_stepping_back", fails: ["nothingSkipped", "keepsNewest"] },
  {
    spec: "logwalk",
    invariants: LOGWALK,
    main: "filtering_stepping_back_before_ids_outrank",
    fails: ["nothingSkipped", "shipsOnce", "keepsNewest"],
  },
  { spec: "logwalk", invariants: LOGWALK, main: "unnumbered_newest_first" },
  { spec: "logwalk", invariants: LOGWALK, main: "unnumbered_newest_first_bursts", fails: ["nothingSkipped"] },
  { spec: "logwalk", invariants: LOGWALK, main: "unnumbered_stepping_back", fails: ["nothingSkipped", "shipsOnce"] },
  { spec: "logwalk", invariants: LOGWALK, main: "paged" },
  { spec: "logwalk", invariants: LOGWALK, main: "budgeted" },
  { spec: "logwalk", invariants: LOGWALK, main: "budgeted_one_document" },
  { spec: "logwalk", invariants: LOGWALK, main: "budgeted_filtering" },
  { spec: "logwalk", invariants: LOGWALK, main: "budgeted_ignoring_skip", fails: ["keepsNewest"] },
  { spec: "logwalk", invariants: LOGWALK, main: "budgeted_newest_first" },
  {
    spec: "logwalk",
    invariants: LOGWALK,
    main: "budgeted_newest_first_before_pages_behind",
    fails: ["cutsOnlyWhenShort"],
  },
  { spec: "collector", invariants: COLLECTOR, main: "fleet" },
  { spec: "collector", invariants: COLLECTOR, main: "fleet_wider" },
  { spec: "collector", invariants: COLLECTOR, main: "fleet_without_id_held", fails: ["inOrder", "oneLifeAtATime"] },
];

// How many traces a replayed instance gives, drawn from this seed.
const SEED = "0x5eed";

// Quint and TLC write beside themselves and under the temporary directory,
// and TLC leaves its states behind when it is stopped.
const scratch = mkdtempSync(join(tmpdir(), "nv-telemetry-spec-"));

function run(args) {
  const done = spawnSync(quint[0], [...quint.slice(1), ...args], {
    cwd: scratch,
    env: { ...process.env, TMPDIR: scratch },
    encoding: "utf8",
  });
  return `${done.stdout ?? ""}${done.stderr ?? ""}${done.error ?? ""}`;
}

function verdict({ spec, main, simulated }, invariants) {
  const path = join(root, "spec", `${spec}.qnt`);
  const how = simulated
    ? ["run", path, "--max-samples=20000", "--max-steps=400"]
    : ["verify", path, "--backend=tlc"];
  const output = run([...how, `--main=${main}`, "--invariants", ...invariants]);
  if (output.includes("[ok]")) return "holds";
  if (output.includes("[violation]")) return "fails";
  console.error(output);
  throw new Error(`${spec}::${main}: Quint gave no verdict`);
}

function check() {
  let wrong = 0;
  const expect = (instance, invariants, wanted) => {
    if (invariants.length === 0) return;
    const got = verdict(instance, invariants);
    const mark = got === wanted ? "ok  " : "WRONG";
    wrong += got === wanted ? 0 : 1;
    console.log(`${mark} ${instance.spec}::${instance.main} ${wanted} ${invariants.join(" ")}`);
  };
  for (const instance of checks) {
    const fails = instance.fails ?? [];
    expect(instance, instance.invariants.filter((name) => !fails.includes(name)), "holds");
    for (const name of fails) expect(instance, [name], "fails");
  }
  if (wrong > 0) throw new Error(`${wrong} checks did not give what the checks table says`);
}

// FNV-1a over the spec's bytes, as the replay test computes it.
function fingerprint(bytes) {
  let hash = 0xcbf29ce484222325n;
  for (const byte of bytes) hash = ((hash ^ BigInt(byte)) * 0x100000001b3n) & 0xffffffffffffffffn;
  return hash.toString(16).padStart(16, "0");
}

const picked = (state, name) => {
  const pick = state["mbt::nondetPicks"][name];
  if (pick.tag !== "Some") throw new Error(`${state["mbt::actionTaken"]} picked no ${name}`);
  return typeof pick.value === "boolean" ? pick.value : Number(pick.value["#bigint"]);
};

// One step of endpoint.qnt as its replay reads it.
const endpointStep = {
  init: () => null,
  tick: () => "t",
  settle: () => "s",
  connect: (state) => `c${picked(state, "s")}`,
  poll: (state) => `p${picked(state, "p")}`,
  givesUp: (state) => `g${picked(state, "s")}`,
  pollEnds: (state) => `e${picked(state, "p")}${picked(state, "failed") ? "-" : "+"}`,
  connectOpens: (state) => `o${picked(state, "s")}`,
  connectFails: (state) =>
    `f${picked(state, "s")}${picked(state, "scoped") ? "s" : "r"}${picked(state, "retryable") ? "+" : "-"}`,
  delivers: (state) => `d${picked(state, "s")}`,
  streamEnds: (state) => `x${picked(state, "s")}${picked(state, "retryable") ? "+" : "-"}`,
};

// One step of stream.qnt, and the cursor it leaves: `L`ive, re`F`used, or
// `R`esume at a boot, an id, and a run. A resume, a refusal, and a gap
// name the position the provider asked the device to resume after.
const number = (value) => Number(value["#bigint"]);
const position = (cursor) => `${number(cursor.value.boot)}.${number(cursor.value.id)}`;
const left = (cursor) =>
  cursor.tag === "Resume" ? `R${position(cursor)}.${number(cursor.value.runId)}` : cursor.tag[0] === "L" ? "L" : "F";
const streamAct = {
  emit: () => "e",
  restart: () => "b",
  openLive: (state, name) => `l${number(state[name("runId")])}`,
  resume: (state, name, before) => `r${position(before[name("cursor")])}`,
  refuse: (state, name, before) => `f${position(before[name("cursor")])}`,
  exceed: (state, name, before) => `x${position(before[name("cursor")])}`,
  deliver: (state, name) => `d${number(state[name("runId")])}.${number(state[name("conn")])}`,
  close: () => "c",
  giveUp: () => "g",
  handBack: () => "h",
};
const streamStep = new Proxy(streamAct, {
  get: (acts, action) => (state, name, before) =>
    action === "init" ? null : `${acts[action](state, name, before)}=${left(state[name("cursor")])}`,
});

// The specs whose traces are replayed: the instances, how many traces of
// how many steps each gives, and the test that follows them.
const replays = [
  {
    spec: "endpoint",
    instances: ["replay", "replay_stopping"],
    traces: 20,
    steps: 400,
    step: endpointStep,
    test: "orchestration/tests/spec_replay.rs",
  },
  {
    spec: "stream",
    instances: ["replay_mock", "replay_bmcweb"],
    traces: 20,
    steps: 200,
    step: streamStep,
    test: "sources/redfish/tests/event_stream_replay.rs",
  },
];

function draw({ spec, instances, traces, steps, step, test }) {
  const path = join(root, "spec", `${spec}.qnt`);
  const source = readFileSync(path);
  const lines = [
    `# Traces of spec/${spec}.qnt, replayed by ${test}.`,
    "# Written by `make spec`. Do not edit.",
    `spec ${fingerprint(source)}`,
  ];
  for (const main of instances) {
    const instance = new RegExp(`module ${main} \\{\\s*import ${spec}\\(([^)]*)\\)`).exec(source.toString());
    if (instance === null) throw new Error(`spec/${spec}.qnt has no instance ${main}`);
    const constants = instance[1].split(",").map((pair) => pair.replace(/\s+/g, ""));
    lines.push(`instance ${main} ${constants.join(" ")}`);
    const output = run([
      "run", path, `--main=${main}`, "--mbt", `--seed=${SEED}`, `--n-traces=${traces}`,
      `--max-samples=${traces}`, `--max-steps=${steps}`, `--out-itf=${spec}_${main}_{seq}.itf.json`,
    ]);
    const files = readdirSync(scratch).filter((name) => name.startsWith(`${spec}_${main}_`)).sort();
    if (files.length !== traces) {
      console.error(output);
      throw new Error(`${main}: wanted ${traces} traces, Quint wrote ${files.length}`);
    }
    const name = (variable) => `${main}::${spec}::${variable}`;
    for (const file of files) {
      const { states } = JSON.parse(readFileSync(join(scratch, file), "utf8"));
      const words = states.map((state, at) => step[state["mbt::actionTaken"]](state, name, states[at - 1]));
      lines.push(`trace ${words.filter((word) => word !== null).join(" ")}`);
    }
  }
  writeFileSync(join(root, "spec", "traces", `${spec}.txt`), `${lines.join("\n")}\n`);
  console.log(`wrote ${instances.length * traces} traces to spec/traces/${spec}.txt`);
}

try {
  if (process.argv[2] !== "traces") check();
  replays.forEach(draw);
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
} finally {
  rmSync(scratch, { recursive: true, force: true });
}
