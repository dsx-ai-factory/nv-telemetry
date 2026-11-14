// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// The collector across endpoints: each endpoint's task produces reports,
// holds them while the one bounded report stream is full, and retires when
// told, when the collector shuts down, or when a unit panics; the embedder
// adds an endpoint again once it has retired; the consumer takes, stalls,
// or goes away. See docs/FORMAL.md for the assumptions (C1, C2), the
// machine (M4) and the properties (P6, P8) named here.
//
// Not modelled: what an endpoint dispatches and when (endpoint.qnt), time,
// an endpoint that retires on its own (lifecycle.qnt), a tokio runtime
// shut down under the tasks, and the reports a panic loses inside the
// dispatcher's runtime.
module collector {
  const ENDPOINTS: int
  const LIVES: int      // times one endpoint may be added
  const REPORTS: int    // reports one life may produce
  const CAPACITY: int   // outputs the report stream holds
  const PANICS: bool    // a unit may panic
  const LEAVES: bool    // the consumer may drop the report stream
  // An endpoint's id stays taken until its retirement is delivered; false
  // frees it once the runtime has shut down, which the collector was built
  // not to do.
  const HOLDS_ID: bool

  // One life of an endpoint: the endpoint and which adding of it this is.
  type Task = (int, int)
  type Phase = Unborn | Running | Stopping | Draining | Done
  type Output =
    | Report({ endpoint: int, life: int, seq: int })
    | Retired({ endpoint: int, life: int, reports: int })

  // The tasks.
  var phase: Task -> Phase
  var inFlight: Task -> int
  var produced: Task -> int
  // The reports a task has that the stream had no room for yet.
  var held: Task -> List[Output]
  // The registry: the life that holds each endpoint's id, zero when it is
  // free, and how many lives each endpoint has had.
  var holder: int -> int
  var lives: int -> int
  var shutDown: bool
  // The report stream, and whether its consumer still has it.
  var stream: List[Output]
  var listening: bool
  // The consumer: per endpoint, the latest life it has heard from, how
  // many of that life's reports it holds, and the latest life whose
  // retirement it has; and whether anything reached it out of place.
  var seen: int -> int
  var got: int -> int
  var gone: int -> int
  var fault: bool

  pure val endpoints = 0.to(ENDPOINTS - 1)
  pure val tasks = tuples(endpoints, 1.to(LIVES))

  pure def max(a: int, b: int): int = if (a > b) a else b

  action init = all {
    phase' = tasks.mapBy(t => Unborn),
    inFlight' = tasks.mapBy(t => 0),
    produced' = tasks.mapBy(t => 0),
    held' = tasks.mapBy(t => []),
    holder' = endpoints.mapBy(e => 0),
    lives' = endpoints.mapBy(e => 0),
    shutDown' = false,
    stream' = [],
    listening' = true,
    seen' = endpoints.mapBy(e => 0),
    got' = endpoints.mapBy(e => 0),
    gone' = endpoints.mapBy(e => 0),
    fault' = false,
  }

  // M4: an endpoint is added while its id is free, the collector is not
  // shut down, and the consumer is there.
  action adds = {
    nondet e = oneOf(endpoints)
    val life = lives.get(e) + 1
    all {
      not(shutDown),
      listening,
      holder.get(e) == 0,
      life <= LIVES,
      holder' = holder.set(e, life),
      lives' = lives.set(e, life),
      phase' = phase.set((e, life), Running),
      inFlight' = inFlight,
      produced' = produced,
      held' = held,
      shutDown' = shutDown,
      stream' = stream,
      listening' = listening,
      seen' = seen,
      got' = got,
      gone' = gone,
      fault' = fault,
    }
  }

  // An acquisition is dispatched. C2: a task that holds reports dispatches
  // only what was already due, which this leaves open.
  action dispatches = {
    nondet t = oneOf(tasks)
    all {
      phase.get(t) == Running,
      inFlight.get(t) == 0,
      produced.get(t) < REPORTS,
      inFlight' = inFlight.set(t, 1),
      phase' = phase,
      produced' = produced,
      held' = held,
      holder' = holder,
      lives' = lives,
      shutDown' = shutDown,
      stream' = stream,
      listening' = listening,
      seen' = seen,
      got' = got,
      gone' = gone,
      fault' = fault,
    }
  }

  // P8: an acquisition that ends is one report, held until delivered. A
  // retiring task lets the one in flight end.
  action completes = {
    nondet t = oneOf(tasks)
    val report = Report({ endpoint: t._1, life: t._2, seq: produced.get(t) + 1 })
    all {
      phase.get(t) == Running or phase.get(t) == Stopping,
      inFlight.get(t) == 1,
      inFlight' = inFlight.set(t, 0),
      produced' = produced.set(t, produced.get(t) + 1),
      held' = held.set(t, held.get(t).append(report)),
      phase' = phase,
      holder' = holder,
      lives' = lives,
      shutDown' = shutDown,
      stream' = stream,
      listening' = listening,
      seen' = seen,
      got' = got,
      gone' = gone,
      fault' = fault,
    }
  }

  // M4: a stream's item is pulled only while the task holds nothing.
  action yields = {
    nondet t = oneOf(tasks)
    val report = Report({ endpoint: t._1, life: t._2, seq: produced.get(t) + 1 })
    all {
      phase.get(t) == Running,
      held.get(t).length() == 0,
      produced.get(t) + inFlight.get(t) < REPORTS,
      produced' = produced.set(t, produced.get(t) + 1),
      held' = held.set(t, [report]),
      phase' = phase,
      inFlight' = inFlight,
      holder' = holder,
      lives' = lives,
      shutDown' = shutDown,
      stream' = stream,
      listening' = listening,
      seen' = seen,
      got' = got,
      gone' = gone,
      fault' = fault,
    }
  }

  // M4: a task hands its oldest held report to the stream when there is
  // room, running or retiring alike.
  action delivers = {
    nondet t = oneOf(tasks)
    all {
      phase.get(t) != Unborn and phase.get(t) != Done,
      held.get(t).length() > 0,
      stream.length() < CAPACITY,
      listening,
      stream' = stream.append(held.get(t).head()),
      held' = held.set(t, held.get(t).tail()),
      phase' = phase,
      inFlight' = inFlight,
      produced' = produced,
      holder' = holder,
      lives' = lives,
      shutDown' = shutDown,
      listening' = listening,
      seen' = seen,
      got' = got,
      gone' = gone,
      fault' = fault,
    }
  }

  // M4: removal reaches the life that holds the id.
  action removes = {
    nondet e = oneOf(endpoints)
    val t = (e, holder.get(e))
    all {
      holder.get(e) != 0,
      phase.get(t) == Running,
      phase' = phase.set(t, Stopping),
      inFlight' = inFlight,
      produced' = produced,
      held' = held,
      holder' = holder,
      lives' = lives,
      shutDown' = shutDown,
      stream' = stream,
      listening' = listening,
      seen' = seen,
      got' = got,
      gone' = gone,
      fault' = fault,
    }
  }

  // M4: shutting down retires every endpoint and refuses new ones.
  action shutsDown = all {
    not(shutDown),
    shutDown' = true,
    phase' = tasks.mapBy(t => if (phase.get(t) == Running) Stopping else phase.get(t)),
    inFlight' = inFlight,
    produced' = produced,
    held' = held,
    holder' = holder,
    lives' = lives,
    stream' = stream,
    listening' = listening,
    seen' = seen,
    got' = got,
    gone' = gone,
    fault' = fault,
  }

  // Where the id is not held to the end, it is free from here.
  def released(t: Task): int -> int =
    if (not(HOLDS_ID) and holder.get(t._1) == t._2) holder.set(t._1, 0) else holder

  // D9, M4: the runtime shuts down once nothing is in flight; what is left
  // is to deliver the reports held, and then the retirement.
  action windsDown = {
    nondet t = oneOf(tasks)
    all {
      phase.get(t) == Stopping,
      inFlight.get(t) == 0,
      phase' = phase.set(t, Draining),
      holder' = released(t),
      inFlight' = inFlight,
      produced' = produced,
      held' = held,
      lives' = lives,
      shutDown' = shutDown,
      stream' = stream,
      listening' = listening,
      seen' = seen,
      got' = got,
      gone' = gone,
      fault' = fault,
    }
  }

  // M4: a panicking unit retires its endpoint as a removal would; what was
  // in flight is lost with the runtime.
  action panics = {
    nondet t = oneOf(tasks)
    all {
      PANICS,
      phase.get(t) == Running,
      inFlight.get(t) == 1,
      phase' = phase.set(t, Draining),
      inFlight' = inFlight.set(t, 0),
      holder' = released(t),
      produced' = produced,
      held' = held,
      lives' = lives,
      shutDown' = shutDown,
      stream' = stream,
      listening' = listening,
      seen' = seen,
      got' = got,
      gone' = gone,
      fault' = fault,
    }
  }

  // P6, M4: the retirement follows the task's last report into the stream,
  // and the id is freed in the same step.
  action retires = {
    nondet t = oneOf(tasks)
    val retired = Retired({ endpoint: t._1, life: t._2, reports: produced.get(t) })
    all {
      phase.get(t) == Draining,
      held.get(t).length() == 0,
      stream.length() < CAPACITY,
      listening,
      stream' = stream.append(retired),
      phase' = phase.set(t, Done),
      holder' = if (holder.get(t._1) == t._2) holder.set(t._1, 0) else holder,
      inFlight' = inFlight,
      produced' = produced,
      held' = held,
      lives' = lives,
      shutDown' = shutDown,
      listening' = listening,
      seen' = seen,
      got' = got,
      gone' = gone,
      fault' = fault,
    }
  }

  // C2: the consumer takes the stream's oldest output, and notes anything
  // out of place: a report after its life's retirement, a life overtaken
  // by a later one, a new life before the last one retired, a gap or a
  // repeat, or a retirement that does not close what the life reported.
  action takes = {
    val judged = match stream.head() {
      | Report(r) =>
        val newer = r.life > seen.get(r.endpoint)
        {
          endpoint: r.endpoint, life: r.life, count: r.seq, retired: false,
          bad: or {
            r.life <= gone.get(r.endpoint),
            r.life < seen.get(r.endpoint),
            newer and gone.get(r.endpoint) != seen.get(r.endpoint),
            r.seq != (if (newer) 0 else got.get(r.endpoint)) + 1,
          },
        }
      | Retired(r) =>
        val newer = r.life > seen.get(r.endpoint)
        {
          endpoint: r.endpoint, life: r.life, count: r.reports, retired: true,
          bad: or {
            r.life <= gone.get(r.endpoint),
            r.life < seen.get(r.endpoint),
            newer and gone.get(r.endpoint) != seen.get(r.endpoint),
            r.reports != (if (newer) 0 else got.get(r.endpoint)),
          },
        }
    }
    all {
      listening,
      stream.length() > 0,
      stream' = stream.tail(),
      seen' = seen.set(judged.endpoint, max(seen.get(judged.endpoint), judged.life)),
      got' = got.set(judged.endpoint, judged.count),
      gone' = if (judged.retired) gone.set(judged.endpoint, judged.life) else gone,
      fault' = (fault or judged.bad),
      phase' = phase,
      inFlight' = inFlight,
      produced' = produced,
      held' = held,
      holder' = holder,
      lives' = lives,
      shutDown' = shutDown,
      listening' = listening,
    }
  }

  // The consumer drops the report stream, and what it held is gone.
  action leaves = all {
    LEAVES,
    listening,
    listening' = false,
    stream' = [],
    phase' = phase,
    inFlight' = inFlight,
    produced' = produced,
    held' = held,
    holder' = holder,
    lives' = lives,
    shutDown' = shutDown,
    seen' = seen,
    got' = got,
    gone' = gone,
    fault' = fault,
  }

  // M4: with nobody listening a task ends without a retirement, and frees
  // its id.
  action abandons = {
    nondet t = oneOf(tasks)
    all {
      not(listening),
      phase.get(t) != Unborn and phase.get(t) != Done,
      phase' = phase.set(t, Done),
      inFlight' = inFlight.set(t, 0),
      held' = held.set(t, []),
      holder' = if (holder.get(t._1) == t._2) holder.set(t._1, 0) else holder,
      produced' = produced,
      lives' = lives,
      shutDown' = shutDown,
      stream' = stream,
      listening' = listening,
      seen' = seen,
      got' = got,
      gone' = gone,
      fault' = fault,
    }
  }

  action step = any {
    adds, dispatches, completes, yields, delivers, removes, shutsDown,
    windsDown, panics, retires, takes, leaves, abandons,
  }

  // P8, P6: every report reaches the consumer once and in its endpoint's
  // order; a retirement closes exactly what its life reported; and nothing
  // of a later life arrives before it.
  val inOrder = not(fault)

  // M4: an id is held by at most the one life that is not over.
  val oneLifeAtATime = endpoints.forall(e =>
    tasks.filter(t => t._1 == e and phase.get(t) != Unborn and phase.get(t) != Done).size() <= 1
  )
}

// Two endpoints, each added up to twice, two reports a life, a report
// stream of one, units that may panic and a consumer that may go away.
module fleet {
  import collector(ENDPOINTS = 2, LIVES = 2, REPORTS = 2, CAPACITY = 1,
    PANICS = true, LEAVES = true, HOLDS_ID = true).*
}

// The same with three lives, three reports a life, and a stream of two.
module fleet_wider {
  import collector(ENDPOINTS = 2, LIVES = 3, REPORTS = 3, CAPACITY = 2,
    PANICS = true, LEAVES = true, HOLDS_ID = true).*
}

// The fleet with ids freed once the runtime has shut down. P6 and P8 fail:
// the endpoint is added again while its old life still holds reports, and
// the new life's reports reach the consumer first. The collector never
// shipped this way; the instance stays to show the model sees what
// holding the id is for.
module fleet_without_id_held {
  import collector(ENDPOINTS = 2, LIVES = 2, REPORTS = 2, CAPACITY = 1,
    PANICS = true, LEAVES = true, HOLDS_ID = false).*
}
