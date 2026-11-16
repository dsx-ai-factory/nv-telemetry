// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// One endpoint's subtree, whole: the bucket over strict priority, stream
// connects ahead of polls and a turn within each, the in-flight bound and
// the breaker over the polls alone, against a device that answers, fails,
// closes, or says nothing. See docs/FORMAL.md for the assumptions (D2-D7,
// D9, E3, E5, E7), the machines (M1, M2, M5) and the properties (P1, P2,
// P3, P7, P10) named here.
//
// Not modelled: stagger (it lengthens every delay by one fixed share),
// retirement (lifecycle.qnt), the consumer (C2), a poll that overlaps its
// own next tick (`pollsDoNotOverlap` says the instance never needed one),
// and anything between ticks: a tick is one refill interval, and requests
// and stream ends land on ticks.
module endpoint {
  // The units and the policy.
  const POLLS: int
  const STREAMS: int
  const MAX_IN_FLIGHT: int
  const BURST: int
  const REFILL: int             // tokens per tick
  const POLL_COST: int
  const CONNECT_COST: int
  const CADENCE: int            // ticks from a poll's dispatch to its next due
  const FIRST_RETRY: int
  const MAX_RETRY: int
  const FLOORED: bool           // the reconnect floor; false is the design before it
  // A connect holds no in-flight slot and passes no breaker; false is the
  // design before it, where the bound and the breaker stood over both.
  const CONNECTS_APART: bool
  const SAMPLE_WINDOW: int
  const MIN_SAMPLES: int
  const TRIP_PERCENT: int       // the failure share that opens the breaker
  const COOL_DOWN: int
  const CONNECT_DEADLINE: int   // ticks until a connect is given up; 0 is the design before it
  // The device and its transport.
  const POLL_TIMEOUT: int       // ticks until a poll is given up; 0 when never
  const SLOW_POLLS: bool        // a poll may stay in flight across ticks
  const SLOW_CONNECTS: bool     // so may a connect
  const POLLS_FAIL: bool        // a poll may fail to reach the endpoint
  const CONNECTS_FAIL: bool     // so may a connect
  const STREAMS_STOP: bool      // a failure may say retrying is pointless
  // The collector reacts to an event before the device does anything more;
  // false leaves their order free. A trace replayed against the code needs
  // it: the replay cannot place a device's act ahead of a dispatch that
  // was already due.
  const EAGER: bool
  // What the properties allow.
  const WINDOW: int             // ticks a due poll may wait while the breaker is closed
  const SILENCE: int            // ticks the polls may go undispatched, breaker or not
  const LATE: int               // ticks a reconnect, or a connect's deadline, may run behind

  type End = { retryable: bool, productive: bool, waited: int }
  // `Due(n)` is due in n ticks; at zero or below it is due and has waited
  // -n. `Connecting` carries the attempt's age, `Streaming` whether the
  // instance delivered.
  type Stream = Due(int) | Connecting(int) | Streaming(bool) | Ended(End) | Stopped
  // `Open` carries the ticks of cool-down left.
  type Breaker = Closed | Open(int) | Probing

  var balance: int
  // Ticks until each poll is due; at zero or below it is due and has
  // waited that long with the breaker closed.
  var pollDue: int -> int
  // The age of each poll in flight; -1 when it is not. Counted only where
  // the transport has a timeout.
  var pollAge: int -> int
  var stream: int -> Stream
  // Instances in a row that delivered nothing, per stream.
  var unproductive: int -> int
  var breaker: Breaker
  // The breaker's window, oldest first; true is a failure.
  var samples: List[bool]
  // Whose turn it is within each class.
  var pollTurn: int
  var streamTurn: int
  // Ticks in a row with a poll due and none dispatched.
  var silent: int

  pure def max(a: int, b: int): int = if (a > b) a else b
  pure def min(a: int, b: int): int = if (a < b) a else b

  pure val polls = 0.to(POLLS - 1)
  pure val streams = 0.to(STREAMS - 1)
  pure val bools = Set(true, false)

  // M1: on a polled endpoint no reconnect comes sooner than the bucket
  // takes to refill twice what all the connects cost.
  pure val floor =
    if (FLOORED and POLLS > 0) (2 * STREAMS * CONNECT_COST + REFILL - 1) / REFILL else 0

  // M2: the delay doubles for each instance in a row that delivered
  // nothing, up to the cap, and the floor applies on top.
  pure def ladder(u: int): int = min(FIRST_RETRY * 2^max(u - 1, 0), MAX_RETRY)
  pure def reconnect(u: int): int = max(ladder(u), floor)
  // Counting stops where the ladder does, which keeps the count finite.
  pure def another(u: int): int = if (u > 0 and ladder(u) == MAX_RETRY) u else u + 1

  pure def connectAge(s: Stream): int = match s {
    | Connecting(age) => age
    | _ => -1
  }

  pure def isDue(s: Stream): bool = match s {
    | Due(n) => n <= 0
    | _ => false
  }

  pure def isEnded(s: Stream): bool = match s {
    | Ended(_) => true
    | _ => false
  }

  // D1: every completion is one sample. D5: a closed breaker opens when
  // the window has enough samples and the failure share reaches the
  // threshold; the probe's own outcome closes it or opens it for a full
  // cool-down; an open one ignores what was dispatched before it opened.
  pure def sampled(b: Breaker, window: List[bool], failed: bool): { breaker: Breaker, samples: List[bool] } =
    match b {
      | Closed =>
        val longer = window.append(failed)
        val kept = if (longer.length() > SAMPLE_WINDOW) longer.tail() else longer
        val failures = kept.select(sample => sample).length()
        if (kept.length() >= MIN_SAMPLES and failures * 100 >= TRIP_PERCENT * kept.length())
          { breaker: Open(COOL_DOWN), samples: [] }
        else
          { breaker: Closed, samples: kept }
      | Probing => { breaker: if (failed) Open(COOL_DOWN) else Closed, samples: [] }
      | Open(_) => { breaker: b, samples: window }
    }

  val pollsOut = polls.filter(p => pollAge.get(p) >= 0).size()
  val connectsOut = streams.filter(s => connectAge(stream.get(s)) >= 0).size()

  // M1: what the in-flight bound and the breaker count.
  val guarded = if (CONNECTS_APART) pollsOut else pollsOut + connectsOut

  // D5: once the cool-down is over and earlier work has drained, one probe.
  val breakerPasses = match breaker {
    | Closed => true
    | Open(left) => left == 0 and guarded == 0
    | Probing => false
  }

  // D2, D5, D6: a readiness scan reaches the polls only past a bucket that
  // is not in debt, the in-flight bound, and the breaker. M1: it reaches
  // the streams past the bucket alone.
  val pollsReached = balance >= 0 and guarded < MAX_IN_FLIGHT and breakerPasses
  val streamsReached = if (CONNECTS_APART) balance >= 0 else pollsReached

  // M2: an ended stream learns its next attempt at the next scan to reach
  // it, and the delay runs from that scan; a connect past its deadline is
  // given up at such a scan too. D9: a runtime whose bucket is short of a
  // token sleeps until it is whole, so between a debt and a token the scan
  // comes only if something else wakes the runtime.
  val unsettled = streams.exists(s => isEnded(stream.get(s)))
  def expired(s: int): bool =
    CONNECT_DEADLINE > 0 and connectAge(stream.get(s)) >= CONNECT_DEADLINE
  val scanOwed = streamsReached and balance >= 1 and
    (unsettled or streams.exists(s => expired(s)))

  // D2, D3: the priority branch hands the bucket no cost hint, so the
  // bucket gates on one whole token and charges the real cost, into debt.
  val connectAdmitted = streamsReached and not(unsettled) and balance >= 1
  val pollAdmitted = pollsReached and not(unsettled) and balance >= 1

  val dueStreams = streams.filter(s => isDue(stream.get(s)))
  val duePolls = polls.filter(p => pollDue.get(p) <= 0 and pollAge.get(p) < 0)

  // D4: within a class, the first with work from whose turn it is.
  pure def turnsAway(turn: int, unit: int, units: int): int = (unit - turn + units) % units
  pure def inTurn(pick: int, turn: int, units: int, ready: Set[int]): bool =
    ready.forall(other => turnsAway(turn, pick, units) <= turnsAway(turn, other, units))

  // D5: the item an open breaker lets through is its probe.
  val probing = match breaker {
    | Open(_) => Probing
    | _ => breaker
  }

  // E5: the transport gives up on a poll at POLL_TIMEOUT, where it has one.
  def pollTimesOut(p: int): bool = POLL_TIMEOUT > 0 and pollAge.get(p) >= POLL_TIMEOUT

  // D3: a due connect is taken before any poll.
  val dispatchDue = or {
    connectAdmitted and dueStreams.size() > 0,
    pollAdmitted and dueStreams.size() == 0 and duePolls.size() > 0,
  }

  // M2: the leaf gives up a connect past its deadline at a scan that
  // reaches it, and the attempt records its end once the runtime has
  // dispatched what it could.
  def givenUp(s: int): bool = expired(s) and streamsReached and not(dispatchDue)

  // The device's turn: always, or only once the collector has nothing to do.
  val deviceActs = not(EAGER) or and {
    not(dispatchDue),
    not(streamsReached and unsettled),
    not(streams.exists(s => givenUp(s))),
  }

  // D9: time passes only when nothing is admissible, no scan is owed, no
  // poll is past its timeout, and a device that is not slow has answered.
  val quiet = and {
    not(dispatchDue),
    not(scanOwed),
    not(polls.exists(p => pollTimesOut(p))),
    SLOW_POLLS or pollsOut == 0,
    SLOW_CONNECTS or connectsOut == 0,
  }

  // D7: a poll is due at once when seated, and so is a connect (M2).
  action init = all {
    balance' = BURST,
    pollDue' = polls.mapBy(p => 0),
    pollAge' = polls.mapBy(p => -1),
    stream' = streams.mapBy(s => Due(0)),
    unproductive' = streams.mapBy(s => 0),
    breaker' = Closed,
    samples' = [],
    pollTurn' = 0,
    streamTurn' = 0,
    silent' = 0,
  }

  // M2: the scan turns every ended instance into its next due tick, or
  // the stop when its failure said retrying is pointless.
  action settle = all {
    streamsReached,
    unsettled,
    stream' = streams.mapBy(s => match stream.get(s) {
      | Ended(end) =>
        if (end.retryable)
          Due(reconnect(if (end.productive) 0 else another(unproductive.get(s))))
        else Stopped
      | _ => stream.get(s)
    }),
    unproductive' = streams.mapBy(s => match stream.get(s) {
      | Ended(end) =>
        if (end.productive or not(end.retryable)) 0 else another(unproductive.get(s))
      | _ => unproductive.get(s)
    }),
    balance' = balance,
    pollDue' = pollDue,
    pollAge' = pollAge,
    breaker' = breaker,
    samples' = samples,
    pollTurn' = pollTurn,
    streamTurn' = streamTurn,
    silent' = silent,
  }

  // D3: a due connect is taken before any poll.
  action connect = {
    nondet s = oneOf(dueStreams)
    all {
      connectAdmitted,
      inTurn(s, streamTurn, STREAMS, dueStreams),
      stream' = stream.set(s, Connecting(0)),
      streamTurn' = (s + 1) % STREAMS,
      balance' = balance - CONNECT_COST,
      breaker' = if (CONNECTS_APART) breaker else probing,
      pollDue' = pollDue,
      pollAge' = pollAge,
      unproductive' = unproductive,
      samples' = samples,
      pollTurn' = pollTurn,
      silent' = silent,
    }
  }

  // D7: the next poll is due a cadence after this dispatch.
  action poll = {
    nondet p = oneOf(duePolls)
    all {
      pollAdmitted,
      dueStreams.size() == 0,
      inTurn(p, pollTurn, POLLS, duePolls),
      pollAge' = pollAge.set(p, 0),
      pollDue' = pollDue.set(p, CADENCE),
      pollTurn' = (p + 1) % POLLS,
      balance' = balance - POLL_COST,
      breaker' = probing,
      silent' = 0,
      stream' = stream,
      unproductive' = unproductive,
      samples' = samples,
      streamTurn' = streamTurn,
    }
  }

  // E7, M5: a poll ends. Only a failure to reach the endpoint, a timeout
  // among them, is a failure to the breaker.
  action pollEnds = {
    nondet p = oneOf(polls)
    nondet failed = oneOf(bools)
    val judged = sampled(breaker, samples, failed)
    all {
      deviceActs,
      pollAge.get(p) >= 0,
      failed implies (POLLS_FAIL or pollTimesOut(p)),
      pollAge' = pollAge.set(p, -1),
      breaker' = judged.breaker,
      samples' = judged.samples,
      balance' = balance,
      pollDue' = pollDue,
      stream' = stream,
      unproductive' = unproductive,
      pollTurn' = pollTurn,
      streamTurn' = streamTurn,
      silent' = silent,
    }
  }

  // M5: the breaker samples a connect only where connects pass it.
  def connectSampled(failed: bool): { breaker: Breaker, samples: List[bool] } =
    if (CONNECTS_APART) { breaker: breaker, samples: samples }
    else sampled(breaker, samples, failed)

  // M2: an opened stream holds no slot and no tokens.
  action connectOpens = {
    nondet s = oneOf(streams)
    val judged = connectSampled(false)
    all {
      deviceActs,
      connectAge(stream.get(s)) >= 0,
      stream' = stream.set(s, Streaming(false)),
      breaker' = judged.breaker,
      samples' = judged.samples,
      balance' = balance,
      pollDue' = pollDue,
      pollAge' = pollAge,
      unproductive' = unproductive,
      pollTurn' = pollTurn,
      streamTurn' = streamTurn,
      silent' = silent,
    }
  }

  // E7, M5: a connect fails. `scoped` is a failure to reach the endpoint.
  action connectFails = {
    nondet s = oneOf(streams)
    nondet scoped = oneOf(bools)
    nondet retryable = oneOf(bools)
    val judged = connectSampled(scoped)
    all {
      deviceActs,
      connectAge(stream.get(s)) >= 0,
      scoped implies CONNECTS_FAIL,
      retryable or STREAMS_STOP,
      stream' = stream.set(s, Ended({ retryable: retryable, productive: false, waited: 0 })),
      breaker' = judged.breaker,
      samples' = judged.samples,
      balance' = balance,
      pollDue' = pollDue,
      pollAge' = pollAge,
      unproductive' = unproductive,
      pollTurn' = pollTurn,
      streamTurn' = streamTurn,
      silent' = silent,
    }
  }

  // M2: the leaf's own timeout is a failure to reach the endpoint, retried.
  action givesUp = {
    nondet s = oneOf(streams)
    val judged = connectSampled(true)
    all {
      givenUp(s),
      stream' = stream.set(s, Ended({ retryable: true, productive: false, waited: 0 })),
      breaker' = judged.breaker,
      samples' = judged.samples,
      balance' = balance,
      pollDue' = pollDue,
      pollAge' = pollAge,
      unproductive' = unproductive,
      pollTurn' = pollTurn,
      streamTurn' = streamTurn,
      silent' = silent,
    }
  }

  action delivers = {
    nondet s = oneOf(streams)
    all {
      deviceActs,
      stream.get(s) == Streaming(false),
      stream' = stream.set(s, Streaming(true)),
      balance' = balance,
      pollDue' = pollDue,
      pollAge' = pollAge,
      unproductive' = unproductive,
      breaker' = breaker,
      samples' = samples,
      pollTurn' = pollTurn,
      streamTurn' = streamTurn,
      silent' = silent,
    }
  }

  // E3, M2: an open stream ends, closed or failed. The breaker does not
  // hear of it.
  action streamEnds = {
    nondet s = oneOf(streams)
    nondet retryable = oneOf(bools)
    nondet productive = oneOf(bools)
    all {
      deviceActs,
      retryable or STREAMS_STOP,
      stream.get(s) == Streaming(productive),
      stream' = stream.set(s, Ended({ retryable: retryable, productive: productive, waited: 0 })),
      balance' = balance,
      pollDue' = pollDue,
      pollAge' = pollAge,
      unproductive' = unproductive,
      breaker' = breaker,
      samples' = samples,
      pollTurn' = pollTurn,
      streamTurn' = streamTurn,
      silent' = silent,
    }
  }

  val closed = breaker == Closed
  // The breaker holds a stream back only where connects pass it.
  val streamsFree = CONNECTS_APART or closed

  action tick = all {
    quiet,
    balance' = min(BURST, balance + REFILL),
    pollDue' = polls.mapBy(p =>
      val due = pollDue.get(p)
      if (due > 0) due - 1
      else if (closed) max(due - 1, -(WINDOW + 1))
      else due
    ),
    pollAge' = polls.mapBy(p =>
      val age = pollAge.get(p)
      if (age >= 0 and POLL_TIMEOUT > 0) age + 1 else age
    ),
    stream' = streams.mapBy(s => match stream.get(s) {
      | Due(n) =>
        if (n > 0) Due(n - 1)
        else if (streamsFree) Due(max(n - 1, -(LATE + 1)))
        else Due(n)
      | Connecting(age) => Connecting(min(age + 1, CONNECT_DEADLINE + LATE + 1))
      | Ended(end) =>
        if (streamsFree) Ended({ ...end, waited: min(end.waited + 1, LATE + 1) })
        else Ended(end)
      | _ => stream.get(s)
    }),
    breaker' = match breaker {
      | Open(left) => Open(max(left - 1, 0))
      | _ => breaker
    },
    silent' = if (polls.exists(p => pollDue.get(p) <= 0)) min(silent + 1, SILENCE + 1) else silent,
    unproductive' = unproductive,
    samples' = samples,
    pollTurn' = pollTurn,
    streamTurn' = streamTurn,
  }

  action step = any {
    settle, connect, poll, givesUp, pollEnds, connectOpens, connectFails,
    delivers, streamEnds, tick,
  }

  // P1, P7: with the breaker closed, a due poll is dispatched within
  // WINDOW ticks.
  val pollServed = polls.forall(p => pollDue.get(p) >= -WINDOW)

  // P2: the bucket's debt never exceeds the largest cost it charges.
  val debtBounded = balance >= 1 - max(CONNECT_COST, POLL_COST)

  // P3: an ended stream is scheduled, a due connect dispatched, and a
  // connect past its deadline given up, within LATE ticks each. Before
  // connects stood apart, only while the breaker was closed.
  val streamReturns = streams.forall(s => match stream.get(s) {
    | Due(n) => n >= -LATE
    | Connecting(age) => age <= CONNECT_DEADLINE + LATE
    | Ended(end) => end.waited <= LATE
    | _ => true
  })

  // P10: the polls never go undispatched for more than SILENCE ticks,
  // whatever the breaker does.
  val pollsNotSilenced = silent <= SILENCE

  // The model's own limit: no poll fell due while its last one was still
  // in flight and a slot was free for it.
  val pollsDoNotOverlap = polls.forall(p =>
    not(pollDue.get(p) <= 0 and pollAge.get(p) >= 0 and guarded < MAX_IN_FLIGHT)
  )
}

// Every instance takes the library's rate policy with one tick a second:
// burst 4, refill 1, a poll costs 1 and is due every 5. The breaker opens
// at half of at least 4 samples, as the library's does, but its window is
// 4 and not 32, and the cool-down, the retry cap, and the connect deadline
// are a few ticks and not 60, 60, and 30: the lists and timers stay short
// enough to check every state.

// One poll and one Redfish event stream, whose connect costs 3, against a
// device that answers at once and is always reachable. The floor is
// 2 x 3 / 1 = 6. No poll waits a tick.
module steady {
  import endpoint(POLLS = 1, STREAMS = 1, MAX_IN_FLIGHT = 1, BURST = 4,
    REFILL = 1, POLL_COST = 1, CONNECT_COST = 3, CADENCE = 5, FIRST_RETRY = 2,
    MAX_RETRY = 8, FLOORED = true, CONNECTS_APART = true, SAMPLE_WINDOW = 4,
    MIN_SAMPLES = 4, TRIP_PERCENT = 50, COOL_DOWN = 8, CONNECT_DEADLINE = 2,
    POLL_TIMEOUT = 0, SLOW_POLLS = false, SLOW_CONNECTS = false,
    POLLS_FAIL = false, CONNECTS_FAIL = false, STREAMS_STOP = true,
    EAGER = false, WINDOW = 0, SILENCE = 0, LATE = 1).*
}

// Two polls and two such streams, so the floor is 12. The four connects
// and polls of a round share one bucket: a poll waits at most 4 ticks.
module two_each {
  import endpoint(POLLS = 2, STREAMS = 2, MAX_IN_FLIGHT = 1, BURST = 4,
    REFILL = 1, POLL_COST = 1, CONNECT_COST = 3, CADENCE = 5, FIRST_RETRY = 2,
    MAX_RETRY = 16, FLOORED = true, CONNECTS_APART = true, SAMPLE_WINDOW = 4,
    MIN_SAMPLES = 4, TRIP_PERCENT = 50, COOL_DOWN = 8, CONNECT_DEADLINE = 2,
    POLL_TIMEOUT = 0, SLOW_POLLS = false, SLOW_CONNECTS = false,
    POLLS_FAIL = false, CONNECTS_FAIL = false, STREAMS_STOP = true,
    EAGER = false, WINDOW = 4, SILENCE = 3, LATE = 3).*
}

// One poll and two streams whose connect costs 1, as a gNMI subscription's
// does. The floor is 2 x 2 / 1 = 4.
module cheap_streams {
  import endpoint(POLLS = 1, STREAMS = 2, MAX_IN_FLIGHT = 1, BURST = 4,
    REFILL = 1, POLL_COST = 1, CONNECT_COST = 1, CADENCE = 5, FIRST_RETRY = 2,
    MAX_RETRY = 8, FLOORED = true, CONNECTS_APART = true, SAMPLE_WINDOW = 4,
    MIN_SAMPLES = 4, TRIP_PERCENT = 50, COOL_DOWN = 8, CONNECT_DEADLINE = 2,
    POLL_TIMEOUT = 0, SLOW_POLLS = false, SLOW_CONNECTS = false,
    POLLS_FAIL = false, CONNECTS_FAIL = false, STREAMS_STOP = true,
    EAGER = false, WINDOW = 0, SILENCE = 0, LATE = 0).*
}

// The same endpoint before the floor. P1 and P10 fail: a connect that costs
// one token leaves no debt, so each stream is scheduled the moment it
// closes, one of the two is due at every tick, and each takes the token
// before the poll can. This instance stays to show the model sees the
// defect. Two streams that cost 3 starve the poll the same way. One such
// stream does not: its connect leaves a debt, and D2 keeps an ended stream
// from being scheduled until the debt is paid.
module cheap_streams_before_floor {
  import endpoint(POLLS = 1, STREAMS = 2, MAX_IN_FLIGHT = 1, BURST = 4,
    REFILL = 1, POLL_COST = 1, CONNECT_COST = 1, CADENCE = 5, FIRST_RETRY = 2,
    MAX_RETRY = 8, FLOORED = false, CONNECTS_APART = true, SAMPLE_WINDOW = 4,
    MIN_SAMPLES = 4, TRIP_PERCENT = 50, COOL_DOWN = 8, CONNECT_DEADLINE = 2,
    POLL_TIMEOUT = 0, SLOW_POLLS = false, SLOW_CONNECTS = false,
    POLLS_FAIL = false, CONNECTS_FAIL = false, STREAMS_STOP = true,
    EAGER = false, WINDOW = 30, SILENCE = 30, LATE = 30).*
}

// One poll and one stream against a device that may fail any request, drop
// the stream, and leave any request unanswered: a poll until the
// transport's timeout of 2, a connect until the leaf's deadline of 2.
// Failing polls open the breaker, and the polls then wait out its
// cool-down: 5 ticks undispatched at most.
module hostile {
  import endpoint(POLLS = 1, STREAMS = 1, MAX_IN_FLIGHT = 1, BURST = 4,
    REFILL = 1, POLL_COST = 1, CONNECT_COST = 3, CADENCE = 5, FIRST_RETRY = 2,
    MAX_RETRY = 8, FLOORED = true, CONNECTS_APART = true, SAMPLE_WINDOW = 4,
    MIN_SAMPLES = 4, TRIP_PERCENT = 50, COOL_DOWN = 8, CONNECT_DEADLINE = 2,
    POLL_TIMEOUT = 2, SLOW_POLLS = true, SLOW_CONNECTS = true,
    POLLS_FAIL = true, CONNECTS_FAIL = true, STREAMS_STOP = true,
    EAGER = false, WINDOW = 0, SILENCE = 5, LATE = 1).*
}

// The same device before the connect deadline. P3 fails: a connect the
// device never answers stays in flight, and its stream never comes back.
// The poll is not held. This instance stays to show the model sees the
// defect.
module hung_connect_before_deadline {
  import endpoint(POLLS = 1, STREAMS = 1, MAX_IN_FLIGHT = 1, BURST = 4,
    REFILL = 1, POLL_COST = 1, CONNECT_COST = 3, CADENCE = 5, FIRST_RETRY = 2,
    MAX_RETRY = 8, FLOORED = true, CONNECTS_APART = true, SAMPLE_WINDOW = 4,
    MIN_SAMPLES = 4, TRIP_PERCENT = 50, COOL_DOWN = 8, CONNECT_DEADLINE = 0,
    POLL_TIMEOUT = 2, SLOW_POLLS = true, SLOW_CONNECTS = true,
    POLLS_FAIL = true, CONNECTS_FAIL = true, STREAMS_STOP = true,
    EAGER = false, WINDOW = 0, SILENCE = 5, LATE = 1).*
}

// The same device before connects stood apart, and before the deadline.
// P7 and P10 fail with P3: the connect holds the one slot, and the poll
// behind it, for good. This instance stays to show the model sees the
// defect.
module hung_connect_before_apart {
  import endpoint(POLLS = 1, STREAMS = 1, MAX_IN_FLIGHT = 1, BURST = 4,
    REFILL = 1, POLL_COST = 1, CONNECT_COST = 3, CADENCE = 5, FIRST_RETRY = 2,
    MAX_RETRY = 8, FLOORED = true, CONNECTS_APART = false, SAMPLE_WINDOW = 4,
    MIN_SAMPLES = 4, TRIP_PERCENT = 50, COOL_DOWN = 8, CONNECT_DEADLINE = 0,
    POLL_TIMEOUT = 2, SLOW_POLLS = true, SLOW_CONNECTS = true,
    POLLS_FAIL = true, CONNECTS_FAIL = true, STREAMS_STOP = true,
    EAGER = false, WINDOW = 30, SILENCE = 80, LATE = 30).*
}

// One poll the device always answers at once, and two streams whose
// connects it may fail. The polls are not held: the breaker is theirs
// alone.
module two_failing_streams {
  import endpoint(POLLS = 1, STREAMS = 2, MAX_IN_FLIGHT = 1, BURST = 4,
    REFILL = 1, POLL_COST = 1, CONNECT_COST = 3, CADENCE = 5, FIRST_RETRY = 2,
    MAX_RETRY = 16, FLOORED = true, CONNECTS_APART = true, SAMPLE_WINDOW = 4,
    MIN_SAMPLES = 4, TRIP_PERCENT = 50, COOL_DOWN = 16, CONNECT_DEADLINE = 2,
    POLL_TIMEOUT = 0, SLOW_POLLS = false, SLOW_CONNECTS = false,
    POLLS_FAIL = false, CONNECTS_FAIL = true, STREAMS_STOP = true,
    EAGER = false, WINDOW = 3, SILENCE = 3, LATE = 3).*
}

// The same endpoint before connects stood apart. P10 fails: a failed
// connect counted against the breaker, and the open breaker's probe was
// whatever strict priority handed it. The stream that failed the last
// probe was scheduled when the cool-down ended, and the other, scheduled a
// cool-down earlier, was due by then and taken first. The two took every
// probe in turn and the poll was not dispatched: for good where the retry
// cap is no longer than the cool-down (16 and 16 here), and otherwise
// until both ladders had passed it. This instance stays to show the model
// sees the defect.
module two_failing_streams_before_apart {
  import endpoint(POLLS = 1, STREAMS = 2, MAX_IN_FLIGHT = 1, BURST = 4,
    REFILL = 1, POLL_COST = 1, CONNECT_COST = 3, CADENCE = 5, FIRST_RETRY = 2,
    MAX_RETRY = 16, FLOORED = true, CONNECTS_APART = false, SAMPLE_WINDOW = 4,
    MIN_SAMPLES = 4, TRIP_PERCENT = 50, COOL_DOWN = 16, CONNECT_DEADLINE = 0,
    POLL_TIMEOUT = 0, SLOW_POLLS = false, SLOW_CONNECTS = false,
    POLLS_FAIL = false, CONNECTS_FAIL = true, STREAMS_STOP = true,
    EAGER = false, WINDOW = 3, SILENCE = 300, LATE = 3).*
}

// Two polls and two streams against the device of `hostile`. Too large to
// check in full; simulated, and its bounds are the least simulation did
// not break, plus a tick or three.
module two_each_hostile {
  import endpoint(POLLS = 2, STREAMS = 2, MAX_IN_FLIGHT = 1, BURST = 4,
    REFILL = 1, POLL_COST = 1, CONNECT_COST = 3, CADENCE = 5, FIRST_RETRY = 2,
    MAX_RETRY = 16, FLOORED = true, CONNECTS_APART = true, SAMPLE_WINDOW = 4,
    MIN_SAMPLES = 4, TRIP_PERCENT = 50, COOL_DOWN = 8, CONNECT_DEADLINE = 2,
    POLL_TIMEOUT = 2, SLOW_POLLS = true, SLOW_CONNECTS = true,
    POLLS_FAIL = true, CONNECTS_FAIL = true, STREAMS_STOP = true,
    EAGER = false, WINDOW = 6, SILENCE = 16, LATE = 4).*
}

// The two instances below are not checked: `make spec` draws traces from
// them, and an orchestration test replays the traces against the
// collector. Their bucket holds an odd balance whatever happens (burst 5,
// refill 2, costs 2 and 4), so it is never at zero and no scan is left to
// chance; with EAGER, a trace then has one reading the code must follow.
// The breaker's window, the cool-down, the timeouts, and the retry cap
// are short so that a trace of a few hundred steps meets them all.

// Two polls and two streams against a device that fails, drops, and
// leaves requests unanswered, with two polls in flight at once.
module replay {
  import endpoint(POLLS = 2, STREAMS = 2, MAX_IN_FLIGHT = 2, BURST = 5,
    REFILL = 2, POLL_COST = 2, CONNECT_COST = 4, CADENCE = 5, FIRST_RETRY = 2,
    MAX_RETRY = 16, FLOORED = true, CONNECTS_APART = true, SAMPLE_WINDOW = 8,
    MIN_SAMPLES = 4, TRIP_PERCENT = 50, COOL_DOWN = 10, CONNECT_DEADLINE = 4,
    POLL_TIMEOUT = 3, SLOW_POLLS = true, SLOW_CONNECTS = true,
    POLLS_FAIL = true, CONNECTS_FAIL = true, STREAMS_STOP = false,
    EAGER = true, WINDOW = 30, SILENCE = 60, LATE = 30).*
}

// The same with one poll in flight at a time and failures that stop a
// stream for good.
module replay_stopping {
  import endpoint(POLLS = 2, STREAMS = 2, MAX_IN_FLIGHT = 1, BURST = 5,
    REFILL = 2, POLL_COST = 2, CONNECT_COST = 4, CADENCE = 5, FIRST_RETRY = 2,
    MAX_RETRY = 16, FLOORED = true, CONNECTS_APART = true, SAMPLE_WINDOW = 8,
    MIN_SAMPLES = 4, TRIP_PERCENT = 50, COOL_DOWN = 10, CONNECT_DEADLINE = 4,
    POLL_TIMEOUT = 3, SLOW_POLLS = true, SLOW_CONNECTS = true,
    POLLS_FAIL = true, CONNECTS_FAIL = true, STREAMS_STOP = true,
    EAGER = true, WINDOW = 30, SILENCE = 60, LATE = 30).*
}
