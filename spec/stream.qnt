// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// One event stream across device restarts: the device's ids and history,
// the provider's cursor and runs, and what reaches the consumer. See
// docs/FORMAL.md for the assumptions (E1-E3, E7, E8, C3), the machines (M2, M3)
// and the properties (P3, P4, P5) named here.
//
// Not modelled: time and the reconnect delay (admission.qnt and the unit
// tests of `ReconnectPolicy::delay`), retirement, payloads that do not
// decode. A connect that fails and may be retried changes nothing here, so
// it has no action.
module stream {
  const TAGGED: bool        // ids carry the device's boot, so a stale one is never held
  const REFUSES: bool       // a position not held is refused; otherwise EventBufferExceeded
  const REOPENS_LIVE: bool  // a refused position reopens live; false is the design before it
  const NEW_RUN_ON_GAP: bool // a reported gap starts a new run; false is the design before it
  const HANDS_BACK_STALE: bool // the embedder may hand back a position older than the last read
  const GIVES_UP: bool      // a failure may say retrying is pointless
  const BUFFER: int         // events the device keeps for replay
  // The bounds below keep the state space finite.
  const MAX_ID: int
  const MAX_BOOT: int
  const MAX_RUN: int

  type Position = { id: int, boot: int, runId: int }
  type Cursor = Live | Resume(Position) | Refused
  type Phase = Due | Streaming | Stopped
  // What the consumer keys on is `runId` and `id`; `boot` is the truth.
  type Record = { runId: int, id: int, boot: int }

  // The device. E1: ids count from one again after every restart.
  var boot: int
  var issued: int
  // The open stream: its run and the last id the device sent on it. Both
  // are zero while no stream is open.
  var runId: int
  var conn: int
  // The provider.
  var phase: Phase
  var cursor: Cursor
  var nextRunId: int
  var fatal: bool
  // The consumer: the records it holds, and the positions the device did
  // not honour that no gap has reported yet.
  var delivered: Set[Record]
  var unreported: int

  pure def isResume(c: Cursor): bool = match c {
    | Resume(_) => true
    | _ => false
  }

  pure def positionOf(c: Cursor): Position = match c {
    | Resume(p) => p
    | _ => { id: 0, boot: 0, runId: 0 }
  }

  val at = positionOf(cursor)

  // E1, E2, E8: whether the device finds the event a position names.
  // Without a tag it compares the id alone, so it finds one it reissued.
  def holds(p: Position): bool = and {
    not(TAGGED) or p.boot == boot,
    p.id <= issued,
    p.id > issued - BUFFER,
  }

  action init = all {
    boot' = 0,
    issued' = 0,
    runId' = 0,
    conn' = 0,
    phase' = Due,
    cursor' = Live,
    nextRunId' = 1,
    fatal' = false,
    delivered' = Set(),
    unreported' = 0,
  }

  // The device issues an event, read or not.
  action emit = all {
    issued < MAX_ID,
    issued' = issued + 1,
    boot' = boot,
    runId' = runId,
    conn' = conn,
    phase' = phase,
    cursor' = cursor,
    nextRunId' = nextRunId,
    fatal' = fatal,
    delivered' = delivered,
    unreported' = unreported,
  }

  // E1: a restart drops the history and any open stream.
  action restart = all {
    boot < MAX_BOOT,
    boot' = boot + 1,
    issued' = 0,
    phase' = if (phase == Streaming) Due else phase,
    runId' = 0,
    conn' = 0,
    cursor' = cursor,
    nextRunId' = nextRunId,
    fatal' = fatal,
    delivered' = delivered,
    unreported' = unreported,
  }

  // M3: with no position, or after a refused one, the stream opens without
  // an id, in a new run. After a refusal its first item is the gap.
  action openLive = all {
    phase == Due,
    cursor == Live or cursor == Refused,
    nextRunId <= MAX_RUN,
    runId' = nextRunId,
    nextRunId' = nextRunId + 1,
    conn' = issued,
    phase' = Streaming,
    unreported' = if (cursor == Refused) unreported - 1 else unreported,
    cursor' = Live,
    boot' = boot,
    issued' = issued,
    fatal' = fatal,
    delivered' = delivered,
  }

  // M3: the device finds the position and the run continues after it.
  action resume = all {
    phase == Due,
    isResume(cursor),
    holds(at),
    runId' = at.runId,
    conn' = at.id,
    phase' = Streaming,
    cursor' = cursor,
    nextRunId' = nextRunId,
    boot' = boot,
    issued' = issued,
    fatal' = fatal,
    delivered' = delivered,
    unreported' = unreported,
  }

  // E2, M3: the device refuses a position it does not hold. The provider
  // drops it and goes on to open live; before that design the refusal was
  // a failure that said retrying is pointless.
  action refuse = all {
    phase == Due,
    isResume(cursor),
    not(holds(at)),
    REFUSES,
    cursor' = if (REOPENS_LIVE) Refused else cursor,
    unreported' = if (REOPENS_LIVE) unreported + 1 else unreported,
    phase' = if (REOPENS_LIVE) Due else Stopped,
    runId' = runId,
    conn' = conn,
    nextRunId' = nextRunId,
    boot' = boot,
    issued' = issued,
    fatal' = fatal,
    delivered' = delivered,
  }

  // E2, M3: the device answers a position it does not hold with
  // EventBufferExceeded, under a fresh id, and goes on from there. The
  // provider reports the gap at once, and the gap starts a new run; before
  // that design the stream stayed in the run it resumed.
  val gapRun = if (NEW_RUN_ON_GAP) nextRunId else at.runId

  action exceed = all {
    phase == Due,
    isResume(cursor),
    not(holds(at)),
    not(REFUSES),
    issued < MAX_ID,
    not(NEW_RUN_ON_GAP) or nextRunId <= MAX_RUN,
    issued' = issued + 1,
    conn' = issued + 1,
    runId' = gapRun,
    cursor' = Resume({ id: issued + 1, boot: boot, runId: gapRun }),
    phase' = Streaming,
    unreported' = unreported,
    nextRunId' = if (NEW_RUN_ON_GAP) nextRunId + 1 else nextRunId,
    boot' = boot,
    fatal' = fatal,
    delivered' = delivered,
  }

  // M3: every payload read moves the cursor and ships under the run.
  action deliver = all {
    phase == Streaming,
    conn < issued,
    conn' = conn + 1,
    delivered' = delivered.union(Set({ runId: runId, id: conn + 1, boot: boot })),
    cursor' = Resume({ id: conn + 1, boot: boot, runId: runId }),
    phase' = phase,
    runId' = runId,
    nextRunId' = nextRunId,
    boot' = boot,
    issued' = issued,
    fatal' = fatal,
    unreported' = unreported,
  }

  // E3: the device closes the stream, and the leaf falls due again.
  action close = all {
    phase == Streaming,
    phase' = Due,
    runId' = 0,
    conn' = 0,
    cursor' = cursor,
    nextRunId' = nextRunId,
    boot' = boot,
    issued' = issued,
    fatal' = fatal,
    delivered' = delivered,
    unreported' = unreported,
  }

  // E7, M2: a failure that says retrying is pointless stops the stream.
  action giveUp = all {
    GIVES_UP,
    phase != Stopped,
    phase' = Stopped,
    fatal' = true,
    runId' = 0,
    conn' = 0,
    cursor' = cursor,
    nextRunId' = nextRunId,
    boot' = boot,
    issued' = issued,
    delivered' = delivered,
    unreported' = unreported,
  }

  // C3: an embedder that restarted hands back a position it kept, which
  // may be older than the last one the stream read.
  action handBack = all {
    HANDS_BACK_STALE,
    phase == Due,
    isResume(cursor),
    at.id > 1,
    cursor' = Resume({ id: at.id - 1, boot: at.boot, runId: at.runId }),
    phase' = phase,
    runId' = runId,
    conn' = conn,
    nextRunId' = nextRunId,
    boot' = boot,
    issued' = issued,
    fatal' = fatal,
    delivered' = delivered,
    unreported' = unreported,
  }

  action step = any {
    emit, restart, openLive, resume, refuse, exceed, deliver, close, giveUp,
    handBack,
  }

  // P3: the stream stops only when a failure said retrying is pointless.
  val stopsOnlyWhenTold = phase == Stopped implies fatal

  // P4: a position the device did not honour is reported as one gap, at
  // the latest when the stream next opens.
  val lossesReported = unreported == (if (cursor == Refused) 1 else 0)

  // P5: a run and an id never name two different events.
  val keysNameOneEvent = delivered.forall(a => delivered.forall(b =>
    (a.runId == b.runId and a.id == b.id) implies a.boot == b.boot
  ))
}

// The bmc-mock device: ids tagged with a generation, a stale one refused.
module mock {
  import stream(TAGGED = true, REFUSES = true, REOPENS_LIVE = true,
    NEW_RUN_ON_GAP = true, HANDS_BACK_STALE = false, GIVES_UP = true,
    BUFFER = 2, MAX_ID = 3, MAX_BOOT = 2, MAX_RUN = 3).*
}

// The same device before a refused position reopened live. P3 fails: the
// first restart stops the stream for good. This instance stays to show the
// model sees the defect.
module mock_before_reopen {
  import stream(TAGGED = true, REFUSES = true, REOPENS_LIVE = false,
    NEW_RUN_ON_GAP = true, HANDS_BACK_STALE = false, GIVES_UP = true,
    BUFFER = 2, MAX_ID = 3, MAX_BOOT = 2, MAX_RUN = 3).*
}

// bmcweb: a plain counter from one, EventBufferExceeded for an id it does
// not find.
module bmcweb {
  import stream(TAGGED = false, REFUSES = false, REOPENS_LIVE = true,
    NEW_RUN_ON_GAP = true, HANDS_BACK_STALE = false, GIVES_UP = true,
    BUFFER = 2, MAX_ID = 3, MAX_BOOT = 2, MAX_RUN = 3).*
}

// bmcweb before a reported gap started a new run. P5 fails: after a restart
// the run continues and its ids come round again. This instance stays to
// show the model sees the defect.
module bmcweb_before_new_run {
  import stream(TAGGED = false, REFUSES = false, REOPENS_LIVE = true,
    NEW_RUN_ON_GAP = false, HANDS_BACK_STALE = false, GIVES_UP = true,
    BUFFER = 2, MAX_ID = 3, MAX_BOOT = 2, MAX_RUN = 3).*
}

// bmcweb with an embedder that hands back an older position. P5 fails: after
// a restart bmcweb finds the id it has issued again, resumes without a sign,
// and the ids the run already delivered come round under it (E8). Open.
module bmcweb_stale_position {
  import stream(TAGGED = false, REFUSES = false, REOPENS_LIVE = true,
    NEW_RUN_ON_GAP = true, HANDS_BACK_STALE = true, GIVES_UP = true,
    BUFFER = 2, MAX_ID = 3, MAX_BOOT = 2, MAX_RUN = 3).*
}

// The same embedder over bmc-mock. A stale position from before a reset is
// refused, so P5 holds.
module mock_stale_position {
  import stream(TAGGED = true, REFUSES = true, REOPENS_LIVE = true,
    NEW_RUN_ON_GAP = true, HANDS_BACK_STALE = true, GIVES_UP = true,
    BUFFER = 2, MAX_ID = 3, MAX_BOOT = 2, MAX_RUN = 3).*
}

// The two instances below are not checked: `make spec` draws traces from
// them, and a test of the Redfish source replays the traces against the
// event stream provider. Their bounds are wide enough for a trace of two
// hundred steps, the embedder hands positions back, and no failure ends
// the stream for good, which would end the trace.

// The bmc-mock device.
module replay_mock {
  import stream(TAGGED = true, REFUSES = true, REOPENS_LIVE = true,
    NEW_RUN_ON_GAP = true, HANDS_BACK_STALE = true, GIVES_UP = false,
    BUFFER = 2, MAX_ID = 40, MAX_BOOT = 12, MAX_RUN = 80).*
}

// bmcweb.
module replay_bmcweb {
  import stream(TAGGED = false, REFUSES = false, REOPENS_LIVE = true,
    NEW_RUN_ON_GAP = true, HANDS_BACK_STALE = true, GIVES_UP = false,
    BUFFER = 2, MAX_ID = 40, MAX_BOOT = 12, MAX_RUN = 80).*
}
