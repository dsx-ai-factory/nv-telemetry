// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// One endpoint from seating to retirement: a poll holds the one in-flight
// slot and a stream connect holds none, the device answers when it likes or
// never, and the embedder removes the endpoint at any moment. See docs/FORMAL.md
// for the assumptions (D6, D9, E5, C2), the machines (M1, M2, M4) and the
// property (P6) named here.
//
// Not modelled: the bucket, the breaker, the connect deadline, and what a
// held slot does to the polls (endpoint.qnt), a consumer that stops
// listening, a unit that panics, stream items. The consumer takes a report before time passes, so
// backpressure never shows as a wait.
module lifecycle {
  const POLLED: bool    // the endpoint has a poll
  const STREAMED: bool  // the endpoint has a stream
  const TIMEOUT: int    // ticks until the transport gives up on a poll; 0 when it never does
  const WAIT: int       // the longest a retirement may wait
  const MAX_AGE: int    // caps a poll's age where no timeout does

  type Endpoint = Collected | Retiring | Retired
  type Poll = Waiting | DuePoll | InFlight
  type Stream = Absent | DueStream | Delayed | Connecting | Streaming | Stopped | Dropped

  var endpoint: Endpoint
  var poll: Poll
  var stream: Stream
  // The age of the poll in flight; zero when there is none.
  var age: int
  // Reports the consumer has not taken yet.
  var held: int
  var removed: bool
  // Ticks since retirement began.
  var retireWait: int

  pure def min(a: int, b: int): int = if (a < b) a else b

  // M1, D6: the default in-flight bound is one, and only a poll holds it.
  val polling = poll == InFlight

  // D9: a retiring runtime admits nothing new.
  val canDispatch = endpoint == Collected and (stream == DueStream or poll == DuePoll)

  // E5: the transport gives up on a poll at TIMEOUT, if it has one.
  val timesOut = TIMEOUT > 0 and polling and age >= TIMEOUT

  // D9, M4: the runtime shuts down once nothing is in flight, the reports
  // held are delivered, and then the retirement is. M2: removal aborted
  // any connect.
  val canRetire = endpoint == Retiring and not(polling) and held == 0

  // M4: an endpoint without polls has nothing left once its stream stopped.
  val allStopped = endpoint == Collected and not(POLLED) and stream == Stopped

  // D9, C2: time passes only when the collector and the consumer have
  // nothing to do.
  val quiet = not(canDispatch or timesOut or held > 0 or canRetire or allStopped)

  action init = all {
    endpoint' = Collected,
    poll' = if (POLLED) DuePoll else Waiting,
    stream' = if (STREAMED) DueStream else Absent,
    age' = 0,
    held' = 0,
    removed' = false,
    retireWait' = 0,
  }

  // M1: a due connect is taken before a due poll.
  action dispatch = all {
    canDispatch,
    stream' = if (stream == DueStream) Connecting else stream,
    poll' = if (stream == DueStream) poll else InFlight,
    age' = if (stream == DueStream) age else 0,
    endpoint' = endpoint,
    held' = held,
    removed' = removed,
    retireWait' = retireWait,
  }

  // The poll ends, by the device's answer or the transport's timeout, and
  // its report waits for the consumer. D7: the next one is due a cadence on.
  action pollEnds = all {
    poll == InFlight,
    poll' = Waiting,
    held' = held + 1,
    age' = 0,
    endpoint' = endpoint,
    stream' = stream,
    removed' = removed,
    retireWait' = retireWait,
  }

  // M2: an opened stream earns no status.
  action connectOpens = all {
    stream == Connecting,
    stream' = Streaming,
    age' = age,
    endpoint' = endpoint,
    poll' = poll,
    held' = held,
    removed' = removed,
    retireWait' = retireWait,
  }

  // M2: a failed connect is a status, and the next attempt waits its delay;
  // a failure that says retrying is pointless stops the stream.
  action connectFails = all {
    stream == Connecting,
    nondet fatal = oneOf(Set(true, false))
    stream' = if (fatal) Stopped else Delayed,
    held' = held + 1,
    age' = age,
    endpoint' = endpoint,
    poll' = poll,
    removed' = removed,
    retireWait' = retireWait,
  }

  // E3: the device closes an open stream.
  action streamCloses = all {
    stream == Streaming,
    stream' = Delayed,
    age' = age,
    endpoint' = endpoint,
    poll' = poll,
    held' = held,
    removed' = removed,
    retireWait' = retireWait,
  }

  // M4, M2: removal drops the stream, which aborts a connect in flight. A
  // poll in flight is left to finish.
  action remove = all {
    endpoint == Collected,
    endpoint' = Retiring,
    removed' = true,
    stream' = if (stream == Absent) Absent else Dropped,
    age' = age,
    poll' = poll,
    held' = held,
    retireWait' = retireWait,
  }

  action retireAlone = all {
    allStopped,
    endpoint' = Retiring,
    poll' = poll,
    stream' = stream,
    age' = age,
    held' = held,
    removed' = removed,
    retireWait' = retireWait,
  }

  // C2: the consumer takes a report.
  action consume = all {
    held > 0,
    held' = held - 1,
    endpoint' = endpoint,
    poll' = poll,
    stream' = stream,
    age' = age,
    removed' = removed,
    retireWait' = retireWait,
  }

  action retire = all {
    canRetire,
    endpoint' = Retired,
    poll' = poll,
    stream' = stream,
    age' = age,
    held' = held,
    removed' = removed,
    retireWait' = retireWait,
  }

  // A cadence and a reconnect delay are one tick each.
  action tick = all {
    quiet,
    endpoint != Retired,
    age' = if (polling) min(age + 1, MAX_AGE) else 0,
    poll' = if (poll == Waiting and POLLED and endpoint == Collected) DuePoll else poll,
    stream' = if (stream == Delayed) DueStream else stream,
    retireWait' = if (endpoint == Retiring) min(retireWait + 1, WAIT + 1) else retireWait,
    endpoint' = endpoint,
    held' = held,
    removed' = removed,
  }

  action step = any {
    dispatch, pollEnds, connectOpens, connectFails, streamCloses, remove,
    retireAlone, consume, retire, tick,
  }

  // P6: a retirement ends within WAIT ticks.
  val retirementEnds = retireWait <= WAIT

  // P6: the retirement follows the endpoint's last report.
  val retiredIsLast = endpoint == Retired implies
    (held == 0 and not(polling) and stream != Connecting)

  // M4: an endpoint retires only when removed, or alone once it has no
  // poll and its stream stopped.
  val retiresOnlyWhenTold = endpoint != Collected implies
    (removed or (not(POLLED) and stream == Stopped))
}

// A transport that gives up after two ticks: the retirement waits no longer.
module with_timeout {
  import lifecycle(POLLED = true, STREAMED = true, TIMEOUT = 2, WAIT = 2,
    MAX_AGE = 3).*
}

// A transport that never gives up. P6 fails: a poll the device never
// answers holds the retirement. Accepted; see docs/FORMAL.md.
module without_timeout {
  import lifecycle(POLLED = true, STREAMED = true, TIMEOUT = 0, WAIT = 2,
    MAX_AGE = 3).*
}

// Streams alone need no timeout to retire: removal aborts the connect.
module stream_only_without_timeout {
  import lifecycle(POLLED = false, STREAMED = true, TIMEOUT = 0, WAIT = 2,
    MAX_AGE = 3).*
}
