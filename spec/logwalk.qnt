// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

// One Redfish log service polled through its cursor: the device writes,
// rotates, and clears entries and moves its clock, and each poll walks the
// collection newest first down to the cursor's member, ships what is new,
// and moves the cursor on. See docs/FORMAL.md for the assumptions (E9-E12),
// the machine (M6) and the properties (P11, P12) named here.
//
// Not modelled: the walk's time budget, a count or a next link the device
// gets wrong, an entry that projects no record, a member that cannot be
// read, a device that refuses or ignores `$filter`, a log that changes
// while it is walked, and a restart, which forgets the cursor.
module logwalk {
  // The device.
  const NEWEST_FIRST: bool    // the collection lists its members newest first
  const IDS_NUMBER: bool      // member ids number the entries as they arrive
  const FILTERS: bool         // the device advertises `$filter` and honours it
  const REPORTS_CLOCK: bool   // the log service reports its clock
  const STEPS_BACK: bool      // the device's clock may step back
  const BURSTS: bool          // two entries may share a second
  const CLEARS: bool          // the log may be cleared
  const REUSES_IDS: bool      // a cleared log numbers its entries from one again
  // Ids that number the entries are not doubted and outrank a probed
  // order; false is the design before it, where a probe outranked them.
  const IDS_OUTRANK: bool
  const CAPACITY: int         // entries the log holds; a full one drops its oldest
  const PAGE: int             // members a page lists; 0 when the collection is one document
  const SKIPS: bool           // the device honours `$skip`
  // The walk.
  const BUDGET: int           // members one walk may read and visit
  // Pages left unread behind the cursor's member are no cut; false is the
  // design before it, where any page left unread was one.
  const PAGES_BEHIND_NO_CUT: bool
  // The bounds below keep the state space finite.
  const MAX_ENTRIES: int      // entries the device ever writes
  const MAX_CLOCK: int
  // What the properties allow.
  const RESHIPS: int          // polls that may ship something already shipped

  // `serial` is the truth: which entry this is, whatever its id and stamp.
  type Entry = { id: int, stamp: int, serial: int }
  type Order = OldestFirst | NewestFirst
  type Position = { member: int, at: int, order: Order, probed: bool, count: int }
  type Cursor = NoCursor | At(Position)
  type Hint = Silent | Says(Order)

  // The device: its log as written, oldest first.
  var log: List[Entry]
  var clock: int
  var nextId: int
  var written: int
  // The provider.
  var cursor: Cursor
  // The consumer: what it holds, how many polls repeated something, counted
  // to one past what is allowed, and whether a poll has run since the
  // device last wrote.
  var shipped: Set[int]
  var reships: int
  var polled: bool
  // What the last step shipped, in the walk's order, and whether it said
  // its walk was cut short: empty and false unless it was a poll.
  var batch: List[Entry]
  var cut: bool
  // The entries the log held and the consumer did not when a poll said it
  // was cut short, and whether a poll that met the cursor's member ever
  // said so with nothing to show.
  var excused: Set[int]
  var alarmed: bool

  pure def max(a: int, b: int): int = if (a > b) a else b
  pure def min(a: int, b: int): int = if (a < b) a else b

  pure def reversed(entries: List[Entry]): List[Entry] =
    entries.foldl([], (turned, entry) => [entry].concat(turned))

  pure def flipped(order: Order): Order = match order {
    | OldestFirst => NewestFirst
    | NewestFirst => OldestFirst
  }

  pure def orElse(hint: Hint, otherwise: Order): Order = match hint {
    | Says(order) => order
    | Silent => otherwise
  }

  // M6: the order a page's ids imply, by the direction most neighbours
  // agree on. Ids that do not number the entries say nothing.
  pure def hinted(page: List[Entry]): Hint =
    if (not(IDS_NUMBER) or page.length() < 2) Silent
    else {
      val pairs = 0.to(page.length() - 2)
      val rising = pairs.filter(i => page[i].id < page[i + 1].id).size()
      val falling = pairs.filter(i => page[i].id > page[i + 1].id).size()
      if (rising > falling) Says(OldestFirst)
      else if (falling > rising) Says(NewestFirst)
      else Silent
    }

  // One pass over the members, newest first as the walk takes it.
  type Pass = {
    done: bool, cursor: Cursor, records: List[Entry], visited: int,
    reached: bool, stalled: bool, refilled: bool, stopped: bool,
  }

  // M6: the budget ends the pass before its next member. The cursor's
  // member under its own stamp ends it too; its id under another stamp is
  // a refilled log, and the cursor is dropped; any other member ships.
  pure def visited(pass: Pass, member: Entry): Pass =
    if (pass.done) pass
    else if (pass.visited >= BUDGET) { ...pass, done: true, stopped: true }
    else {
      val shipping = { ...pass, records: pass.records.append(member), visited: pass.visited + 1 }
      match pass.cursor {
        | NoCursor => shipping
        | At(position) =>
          if (member.id != position.member) shipping
          else if (member.stamp == position.at)
            { ...pass, done: true, reached: true, stalled: pass.visited == 0, visited: pass.visited + 1 }
          else { ...shipping, cursor: NoCursor, refilled: true }
      }
    }

  // `total` is what the collection, or the filtered answer, holds, and
  // `read` how much of it the pass had before it. `otherEnd` is the member
  // at the end not taken for newest, when the pages reach it.
  type Visit = {
    records: List[Entry], position: Cursor, order: Order, trusted: bool,
    filtered: bool, reached: bool, stalled: bool, refilled: bool,
    total: int, read: int, otherEnd: List[Entry],
    jumpedToCursor: bool, stopped: bool, cut: bool,
  }

  // M6: one pass. What is opened is the `$filter` answer from the cursor's
  // second when the poll filters and that answer is not empty, else the
  // collection. The order is the forced one, else the first page's ids',
  // else the one the cursor remembers, else oldest first.
  //
  // E13: a collection longer than a page is listed in pages. An
  // oldest-first one is entered at its tail with `$skip`, where the device
  // honours it: a budget's worth before the end when it holds more than
  // the budget, or where the cursor's member last sat, when `jumping`, if
  // that is later. Pages are then followed until the last, or until a
  // budget's worth of members has been read.
  pure def visit(listing: List[Entry], previous: Cursor, forced: Hint, filtering: bool, jumping: bool): Visit =
    val answer = match previous {
      | At(position) => if (filtering) listing.select(entry => entry.stamp >= position.at) else []
      | NoCursor => []
    }
    val filtered = answer.length() > 0
    val opened = if (filtered) answer else listing
    val total = opened.length()
    val paged = PAGE > 0 and total > PAGE
    val hint = hinted(if (paged) opened.slice(0, PAGE) else opened)
    val chosen = match forced {
      | Says(order) => { order: order, probed: true }
      | Silent => match previous {
        | At(position) =>
          if (position.probed and not(IDS_OUTRANK and hint != Silent))
            { order: position.order, probed: true }
          else { order: orElse(hint, position.order), probed: false }
        | NoCursor => { order: orElse(hint, OldestFirst), probed: false }
      }
    }
    val cursorIndex = match previous {
      | At(position) => if (jumping and not(filtered)) position.count - 1 else -1
      | NoCursor => -1
    }
    val budgetSkip = if (total > BUDGET) total - BUDGET else -1
    val cursorSkip = if (cursorIndex >= PAGE and cursorIndex < total) cursorIndex else -1
    val skip = max(budgetSkip, cursorSkip)
    val jumped = paged and chosen.order == OldestFirst and skip > 0 and SKIPS
    val offset = if (jumped) skip else 0
    val read = if (paged) min(total - offset, (BUDGET + PAGE - 1) / PAGE * PAGE) else total
    val incomplete = offset + read < total
    val window = opened.slice(offset, offset + read)
    val newestFirst = if (chosen.order == OldestFirst) reversed(window) else window
    val pass = newestFirst.foldl({
      done: false, cursor: previous, records: [], visited: 0,
      reached: false, stalled: false, refilled: false, stopped: false,
    }, (pass, member) => visited(pass, member))
    // A filtered answer counts what matched, so the count stays the
    // cursor's own.
    val count = match pass.cursor {
      | At(position) => if (filtered) position.count else total
      | NoCursor => total
    }
    val position = if (pass.records.length() > 0)
      At({
        member: pass.records[0].id, at: pass.records[0].stamp,
        order: chosen.order, probed: chosen.probed, count: count,
      })
    else match pass.cursor {
      | At(kept) => At({ ...kept, order: chosen.order, probed: chosen.probed, count: count })
      | NoCursor => NoCursor
    }
    {
      records: pass.records, position: position, order: chosen.order, trusted: hint != Silent,
      filtered: filtered, reached: pass.reached, stalled: pass.stalled, refilled: pass.refilled,
      total: total, read: read,
      otherEnd: if (total == 0) []
        else if (chosen.order == OldestFirst) [opened[0]]
        else if (incomplete) []
        else [opened[total - 1]],
      jumpedToCursor: jumped and cursorSkip == skip,
      stopped: pass.stopped,
      // M6: a walk says it was cut short when it did not meet the cursor's
      // member and left members unread: those a jump skipped, those the
      // budget cut, or pages it did not read. Pages left unread in a
      // collection listed oldest first hold its newest members, and are a
      // cut whatever the walk met.
      cut: if (PAGES_BEHIND_NO_CUT)
          (incomplete and chosen.order == OldestFirst) or
            (not(pass.reached) and (incomplete or offset > 0 or pass.visited < read))
        else incomplete or (not(pass.reached) and (offset > 0 or pass.visited < read)),
    }

  // M6: a jump that does not find the cursor's member where the collection
  // last held it reads the collection from its head instead, unfiltered,
  // unless the budget stopped it. Then, where the ids say nothing, a walk
  // doubts its order when the end it took for newest held only the
  // cursor's member while the collection grew, or the filtered answer was
  // wider than that member; or when the cursor's member is gone from a
  // collection that did not grow. It reads the member at the other end
  // and, if that one is stamped later, walks again the other way round,
  // unfiltered.
  pure def walk(listing: List[Entry], previous: Cursor, filtering: bool): Visit =
    val jumped = visit(listing, previous, Silent, filtering, true)
    val missed = jumped.jumpedToCursor and not(jumped.stopped) and
      (not(jumped.reached) or jumped.refilled)
    val first = if (missed) visit(listing, previous, Silent, false, false) else jumped
    match previous {
      | NoCursor => first
      | At(position) =>
        val doubted = if (first.filtered) first.stalled and first.read > 1
          else or {
            first.stalled and first.total > position.count,
            not(first.reached) and not(first.refilled) and first.total > 0 and
              first.total == position.count,
          }
        val reference = if (first.records.length() > 0) first.records[0].stamp else position.at
        val trusted = IDS_OUTRANK and first.trusted
        if (doubted and not(trusted) and first.otherEnd.length() > 0 and
            first.otherEnd[0].stamp > reference)
          visit(listing, previous, Says(flipped(first.order)), false, false)
        else first
    }

  action init = all {
    log' = [],
    clock' = 0,
    nextId' = 1,
    written' = 0,
    cursor' = NoCursor,
    shipped' = Set(),
    reships' = 0,
    polled' = true,
    batch' = [],
    cut' = false,
    excused' = Set(),
    alarmed' = false,
  }

  // E9: the device writes an entry under the next id and its clock's
  // stamp; a full log drops its oldest. Without bursts the clock has moved
  // on by the next entry.
  action writes = all {
    written < MAX_ENTRIES,
    BURSTS or clock < MAX_CLOCK,
    val entry = { id: nextId, stamp: clock, serial: written + 1 }
    log' = (if (log.length() == CAPACITY) log.tail() else log).append(entry),
    nextId' = nextId + 1,
    written' = written + 1,
    polled' = false,
    batch' = [],
    cut' = false,
    excused' = excused,
    alarmed' = alarmed,
    clock' = if (BURSTS) clock else clock + 1,
    cursor' = cursor,
    shipped' = shipped,
    reships' = reships,
  }

  action clockTicks = all {
    clock < MAX_CLOCK,
    clock' = clock + 1,
    log' = log,
    nextId' = nextId,
    written' = written,
    cursor' = cursor,
    shipped' = shipped,
    reships' = reships,
    polled' = polled,
    batch' = [],
    cut' = false,
    excused' = excused,
    alarmed' = alarmed,
  }

  // E10: a device without a battery-backed clock restarts in the past.
  action clockStepsBack = {
    nondet earlier = oneOf(0.to(MAX_CLOCK))
    all {
      STEPS_BACK,
      earlier < clock,
      clock' = earlier,
      log' = log,
      nextId' = nextId,
      written' = written,
      cursor' = cursor,
      shipped' = shipped,
      reships' = reships,
      polled' = polled,
      batch' = [],
      cut' = false,
      excused' = excused,
      alarmed' = alarmed,
    }
  }

  // E11: the log is cleared, and may number its entries from one again.
  action clears = all {
    CLEARS,
    log.length() > 0,
    log' = [],
    nextId' = if (REUSES_IDS) 1 else nextId,
    clock' = clock,
    written' = written,
    cursor' = cursor,
    shipped' = shipped,
    reships' = reships,
    polled' = polled,
    batch' = [],
    cut' = false,
    excused' = excused,
    alarmed' = alarmed,
  }

  // M6: a poll filters when the device does and the cursor has a stamp,
  // unless the service's own clock reads before that stamp. What the walk
  // projects ships, and the cursor then moves.
  action polls = {
    val behind = match cursor {
      | At(position) => REPORTS_CLOCK and clock < position.at
      | NoCursor => false
    }
    val listing = if (NEWEST_FIRST) reversed(log) else log
    val result = walk(listing, cursor, FILTERS and not(behind))
    val serials = result.records.indices().map(i => result.records[i].serial)
    val missing = log.indices().map(i => log[i].serial).exclude(shipped.union(serials))
    all {
      cursor' = result.position,
      shipped' = shipped.union(serials),
      reships' = if (serials.exists(serial => shipped.contains(serial))) min(reships + 1, RESHIPS + 1)
        else reships,
      polled' = true,
      batch' = result.records,
      cut' = result.cut,
      excused' = if (result.cut) excused.union(missing) else excused,
      alarmed' = (alarmed or (result.cut and result.reached and missing.size() == 0)),
      log' = log,
      clock' = clock,
      nextId' = nextId,
      written' = written,
    }
  }

  action step = any { writes, clockTicks, clockStepsBack, clears, polls }

  def held(i: int): bool = shipped.contains(log[i].serial)

  // P11: once a poll has run, the consumer holds every entry the log does,
  // but for those the log held when a poll said it was cut short.
  val nothingSkipped = polled implies
    log.indices().forall(i => held(i) or excused.contains(log[i].serial))

  // P12: no more than RESHIPS polls ship an entry the consumer has.
  val shipsOnce = reships <= RESHIPS

  // P13: what a poll leaves behind is older than what it ships: once a
  // poll has run, no entry the consumer lacks is newer than one of the
  // poll's.
  val keepsNewest = polled implies
    log.indices().forall(i => log.indices().forall(j =>
      (not(held(i)) and batch.select(shipped => shipped == log[j]).length() > 0) implies i < j
    ))

  // P14: a poll that met the cursor's member says it was cut short only
  // while the log holds something the consumer does not. One the budget
  // stopped first cannot tell, and says so.
  val cutsOnlyWhenShort = not(alarmed)
}

// The instances below read the collection as one document, with a budget
// it never spends. Each holds a log of three entries, writes five, and has
// four seconds on its clock: small enough to check every state, and enough
// for the log to rotate, be cleared, and be written to again.

// A log that lists oldest first under numbered ids, with a clock that only
// moves on: the common device.
module plain {
  import logwalk(NEWEST_FIRST = false, IDS_NUMBER = true, FILTERS = false,
    REPORTS_CLOCK = false, STEPS_BACK = false, BURSTS = true, CLEARS = true,
    REUSES_IDS = false, IDS_OUTRANK = true, CAPACITY = 3, PAGE = 0,
    SKIPS = true, BUDGET = 99, PAGES_BEHIND_NO_CUT = true, MAX_ENTRIES = 5,
    MAX_CLOCK = 3, RESHIPS = 0).*
}

// The same log listed newest first. The ids say so.
module newest_first {
  import logwalk(NEWEST_FIRST = true, IDS_NUMBER = true, FILTERS = false,
    REPORTS_CLOCK = false, STEPS_BACK = false, BURSTS = true, CLEARS = true,
    REUSES_IDS = false, IDS_OUTRANK = true, CAPACITY = 3, PAGE = 0,
    SKIPS = true, BUDGET = 99, PAGES_BEHIND_NO_CUT = true, MAX_ENTRIES = 5,
    MAX_CLOCK = 3, RESHIPS = 0).*
}

// A device that honours `$filter` and reports its clock.
module filtering {
  import logwalk(NEWEST_FIRST = false, IDS_NUMBER = true, FILTERS = true,
    REPORTS_CLOCK = true, STEPS_BACK = false, BURSTS = true, CLEARS = true,
    REUSES_IDS = false, IDS_OUTRANK = true, CAPACITY = 3, PAGE = 0,
    SKIPS = true, BUDGET = 99, PAGES_BEHIND_NO_CUT = true, MAX_ENTRIES = 5,
    MAX_CLOCK = 3, RESHIPS = 0).*
}

// A cleared log that numbers its entries from one again, one entry a
// second. The stamp tells a refilled log apart.
module refilled {
  import logwalk(NEWEST_FIRST = false, IDS_NUMBER = true, FILTERS = false,
    REPORTS_CLOCK = false, STEPS_BACK = false, BURSTS = false, CLEARS = true,
    REUSES_IDS = true, IDS_OUTRANK = true, CAPACITY = 3, PAGE = 0,
    SKIPS = true, BUDGET = 99, PAGES_BEHIND_NO_CUT = true, MAX_ENTRIES = 5,
    MAX_CLOCK = 6, RESHIPS = 0).*
}

// The same with entries that share a second. P11 fails, by design: a
// refill that lands the cursor's id on the cursor's second looks like the
// cursor's member, and what lies behind it is not shipped. A limit, not a
// defect to fix.
module refilled_same_second {
  import logwalk(NEWEST_FIRST = false, IDS_NUMBER = true, FILTERS = false,
    REPORTS_CLOCK = false, STEPS_BACK = false, BURSTS = true, CLEARS = true,
    REUSES_IDS = true, IDS_OUTRANK = true, CAPACITY = 3, PAGE = 0,
    SKIPS = true, BUDGET = 99, PAGES_BEHIND_NO_CUT = true, MAX_ENTRIES = 5,
    MAX_CLOCK = 3, RESHIPS = 0).*
}

// The common device with a clock that may step back, in either order of
// listing below. Its ids number the entries, so stamps are never asked.
module stepping_back {
  import logwalk(NEWEST_FIRST = false, IDS_NUMBER = true, FILTERS = false,
    REPORTS_CLOCK = false, STEPS_BACK = true, BURSTS = true, CLEARS = true,
    REUSES_IDS = false, IDS_OUTRANK = true, CAPACITY = 3, PAGE = 0,
    SKIPS = true, BUDGET = 99, PAGES_BEHIND_NO_CUT = true, MAX_ENTRIES = 5,
    MAX_CLOCK = 3, RESHIPS = 0).*
}

// The same, listed newest first.
module stepping_back_newest_first {
  import logwalk(NEWEST_FIRST = true, IDS_NUMBER = true, FILTERS = false,
    REPORTS_CLOCK = false, STEPS_BACK = true, BURSTS = true, CLEARS = true,
    REUSES_IDS = false, IDS_OUTRANK = true, CAPACITY = 3, PAGE = 0,
    SKIPS = true, BUDGET = 99, PAGES_BEHIND_NO_CUT = true, MAX_ENTRIES = 5,
    MAX_CLOCK = 3, RESHIPS = 0).*
}

// The same device before numbered ids outranked a probe. P11 and P12 fail.
// A walk that doubted its order read the other end's stamp, and after a
// step back the older end is stamped later, so the walk took the order for
// newest first, shipped the window again, and anchored the cursor on the
// oldest entry. The probed order outranked the ids, so every poll from then
// on stopped at that entry, and what the device wrote was not shipped until
// its clock passed the old stamps. This instance stays to show the model
// sees the defect.
module stepping_back_before_ids_outrank {
  import logwalk(NEWEST_FIRST = false, IDS_NUMBER = true, FILTERS = false,
    REPORTS_CLOCK = false, STEPS_BACK = true, BURSTS = true, CLEARS = true,
    REUSES_IDS = false, IDS_OUTRANK = false, CAPACITY = 3, PAGE = 0,
    SKIPS = true, BUDGET = 99, PAGES_BEHIND_NO_CUT = true, MAX_ENTRIES = 5,
    MAX_CLOCK = 3, RESHIPS = 0).*
}

// The filtering device with such a clock. Nothing ships twice. P11 and P13
// fail, by design: the filter hides an entry stamped behind the cursor, and
// the service's clock tells only if it still reads behind at the poll. A
// limit, not a defect to fix.
module filtering_stepping_back {
  import logwalk(NEWEST_FIRST = false, IDS_NUMBER = true, FILTERS = true,
    REPORTS_CLOCK = true, STEPS_BACK = true, BURSTS = true, CLEARS = true,
    REUSES_IDS = false, IDS_OUTRANK = true, CAPACITY = 3, PAGE = 0,
    SKIPS = true, BUDGET = 99, PAGES_BEHIND_NO_CUT = true, MAX_ENTRIES = 5,
    MAX_CLOCK = 3, RESHIPS = 0).*
}

// The same before numbered ids outranked a probe. P12 fails as well, and
// an idle poll was enough: once the cursor was on an entry stamped after
// the step, the filter from its stamp answered the whole older log, wider
// than the cursor's member, which was the doubt. This instance stays to
// show the model sees the defect.
module filtering_stepping_back_before_ids_outrank {
  import logwalk(NEWEST_FIRST = false, IDS_NUMBER = true, FILTERS = true,
    REPORTS_CLOCK = true, STEPS_BACK = true, BURSTS = true, CLEARS = true,
    REUSES_IDS = false, IDS_OUTRANK = false, CAPACITY = 3, PAGE = 0,
    SKIPS = true, BUDGET = 99, PAGES_BEHIND_NO_CUT = true, MAX_ENTRIES = 5,
    MAX_CLOCK = 3, RESHIPS = 0).*
}

// A log listed newest first whose ids say nothing, one entry a second. The
// first walk takes it for oldest first, and stamps correct it: within two
// polls that ship again what the cursor was anchored behind.
module unnumbered_newest_first {
  import logwalk(NEWEST_FIRST = true, IDS_NUMBER = false, FILTERS = false,
    REPORTS_CLOCK = false, STEPS_BACK = false, BURSTS = false, CLEARS = true,
    REUSES_IDS = false, IDS_OUTRANK = true, CAPACITY = 3, PAGE = 0,
    SKIPS = true, BUDGET = 99, PAGES_BEHIND_NO_CUT = true, MAX_ENTRIES = 5,
    MAX_CLOCK = 6, RESHIPS = 2).*
}

// The same log with entries that share a second. P11 fails, by design: the
// order is told by a later stamp, and until one comes an entry beside the
// cursor's is not shipped. A limit, not a defect to fix.
module unnumbered_newest_first_bursts {
  import logwalk(NEWEST_FIRST = true, IDS_NUMBER = false, FILTERS = false,
    REPORTS_CLOCK = false, STEPS_BACK = false, BURSTS = true, CLEARS = true,
    REUSES_IDS = false, IDS_OUTRANK = true, CAPACITY = 3, PAGE = 0,
    SKIPS = true, BUDGET = 99, PAGES_BEHIND_NO_CUT = true, MAX_ENTRIES = 5,
    MAX_CLOCK = 3, RESHIPS = 2).*
}

// Ids that say nothing and a clock that steps back. P11 and P12 fail, by
// design: only stamps are left to tell the order, and after a step back
// they tell it wrong. A limit, not a defect to fix.
module unnumbered_stepping_back {
  import logwalk(NEWEST_FIRST = false, IDS_NUMBER = false, FILTERS = false,
    REPORTS_CLOCK = false, STEPS_BACK = true, BURSTS = true, CLEARS = true,
    REUSES_IDS = false, IDS_OUTRANK = true, CAPACITY = 3, PAGE = 0,
    SKIPS = true, BUDGET = 99, PAGES_BEHIND_NO_CUT = true, MAX_ENTRIES = 5,
    MAX_CLOCK = 3, RESHIPS = 0).*
}

// The instances below hold five entries, write seven, and list two a page,
// under a budget of three unless they say otherwise, with two seconds on
// the clock.

// The common device, paged, with a budget it never spends: the walk enters
// the collection where the cursor's member last sat.
module paged {
  import logwalk(NEWEST_FIRST = false, IDS_NUMBER = true, FILTERS = false,
    REPORTS_CLOCK = false, STEPS_BACK = false, BURSTS = true, CLEARS = true,
    REUSES_IDS = false, IDS_OUTRANK = true, CAPACITY = 5, PAGE = 2,
    SKIPS = true, BUDGET = 99, PAGES_BEHIND_NO_CUT = true, MAX_ENTRIES = 7,
    MAX_CLOCK = 1, RESHIPS = 0).*
}

// The common device, paged, under the budget: the walk enters a budget's
// worth before the end, and what it cuts is the oldest.
module budgeted {
  import logwalk(NEWEST_FIRST = false, IDS_NUMBER = true, FILTERS = false,
    REPORTS_CLOCK = false, STEPS_BACK = false, BURSTS = true, CLEARS = true,
    REUSES_IDS = false, IDS_OUTRANK = true, CAPACITY = 5, PAGE = 2,
    SKIPS = true, BUDGET = 3, PAGES_BEHIND_NO_CUT = true, MAX_ENTRIES = 7,
    MAX_CLOCK = 1, RESHIPS = 0).*
}

// The same in one document.
module budgeted_one_document {
  import logwalk(NEWEST_FIRST = false, IDS_NUMBER = true, FILTERS = false,
    REPORTS_CLOCK = false, STEPS_BACK = false, BURSTS = true, CLEARS = true,
    REUSES_IDS = false, IDS_OUTRANK = true, CAPACITY = 5, PAGE = 0,
    SKIPS = true, BUDGET = 3, PAGES_BEHIND_NO_CUT = true, MAX_ENTRIES = 7,
    MAX_CLOCK = 1, RESHIPS = 0).*
}

// The same, paged, on a device that filters.
module budgeted_filtering {
  import logwalk(NEWEST_FIRST = false, IDS_NUMBER = true, FILTERS = true,
    REPORTS_CLOCK = true, STEPS_BACK = false, BURSTS = true, CLEARS = true,
    REUSES_IDS = false, IDS_OUTRANK = true, CAPACITY = 5, PAGE = 2,
    SKIPS = true, BUDGET = 3, PAGES_BEHIND_NO_CUT = true, MAX_ENTRIES = 7,
    MAX_CLOCK = 1, RESHIPS = 0).*
}

// A device that ignores `$skip`. P13 fails, by design: the walk can only
// read the pages from the start, the budget ends them before the newest
// entries, and every poll says so. A limit, not a defect to fix.
module budgeted_ignoring_skip {
  import logwalk(NEWEST_FIRST = false, IDS_NUMBER = true, FILTERS = false,
    REPORTS_CLOCK = false, STEPS_BACK = false, BURSTS = true, CLEARS = true,
    REUSES_IDS = false, IDS_OUTRANK = true, CAPACITY = 5, PAGE = 2,
    SKIPS = false, BUDGET = 3, PAGES_BEHIND_NO_CUT = true, MAX_ENTRIES = 7,
    MAX_CLOCK = 1, RESHIPS = 0).*
}

// The paged log listed newest first. Its pages are read up to the budget
// before any member is visited; those left unread are older than the
// cursor's member once the walk has met it, and no cut.
module budgeted_newest_first {
  import logwalk(NEWEST_FIRST = true, IDS_NUMBER = true, FILTERS = false,
    REPORTS_CLOCK = false, STEPS_BACK = false, BURSTS = true, CLEARS = true,
    REUSES_IDS = false, IDS_OUTRANK = true, CAPACITY = 5, PAGE = 2,
    SKIPS = true, BUDGET = 3, PAGES_BEHIND_NO_CUT = true, MAX_ENTRIES = 7,
    MAX_CLOCK = 1, RESHIPS = 0).*
}

// The same before pages behind the cursor's member stopped being a cut.
// P14 fails: pages left unread were reported whatever the walk met, so a
// log longer than the budget said it was cut short on every poll. This
// instance stays to show the model sees the defect.
module budgeted_newest_first_before_pages_behind {
  import logwalk(NEWEST_FIRST = true, IDS_NUMBER = true, FILTERS = false,
    REPORTS_CLOCK = false, STEPS_BACK = false, BURSTS = true, CLEARS = true,
    REUSES_IDS = false, IDS_OUTRANK = true, CAPACITY = 5, PAGE = 2,
    SKIPS = true, BUDGET = 3, PAGES_BEHIND_NO_CUT = false, MAX_ENTRIES = 7,
    MAX_CLOCK = 1, RESHIPS = 0).*
}
