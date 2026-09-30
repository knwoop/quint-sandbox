// A minimal model of a transactional outbox (fast-path + polling Message Relay).
//
// Time is an integer tick. Each component's work is split into actions at the points where it can fail.
// One Spanner transaction or one mutation is a single atomic action.
module outbox {
  // ---- Parameters ----
  const MSGS: Set[int]            // Message IDs (also the tiebreak for rows with the same CreateTime)
  const RELAYS: Set[str]          // Message Relay runs that may overlap
  const SHARD_OF: int -> int      // The generated Shard column, derived from the ID
  const POISON: Set[int]          // Messages whose publish always fails
  const BATCH: int                // Rows picked per shard in one run (batch limit)
  const LEASE: int                // ProcessingExpirationTime = now + LEASE
  const TTL: int                  // ROW DELETION POLICY (OLDER_THAN(SendTime, TTL))
  const MAX_TIME: int             // Upper bound on time to keep exploration finite
  const ATOMIC_CONSUMER: bool     // Whether the consumer checks and applies atomically

  pure val NULL = -1

  type Row = { createTime: int, sendTime: int, procExp: int }
  type Db = { rows: int -> Row, deleted: Set[int], business: Set[int] }
  // fast-path progress: "none" | "committed" | "published" | "done"
  type Relay = { pending: Set[int], claimed: Set[int], published: Set[int] }
  type PubSub = { accepted: Set[int], copies: int -> int }
  type Consumer = { handling: int -> int, processed: Set[int], effects: int -> int }

  // ---- State ----
  var now: int
  var db: Db                      // Business table + OutboxMessages (same Spanner DB)
  var fast: int -> str            // Producer / fast-path progress per message
  var relays: str -> Relay        // In-flight Message Relay runs
  var pubsub: PubSub              // Accepted IDs and unacked delivery copies
  var consumer: Consumer          // In-progress handlers, processed IDs, side-effect counts

  pure val IDLE_RELAY: Relay = { pending: Set(), claimed: Set(), published: Set() }

  // ---- Helpers ----
  pure def isNull(t: int): bool = t == NULL

  pure def published(ps: PubSub, m: int): PubSub =
    { accepted: ps.accepted.union(Set(m)), copies: ps.copies.setBy(m, c => c + 1) }

  // Unsent and not being processed (the WHERE clause of the polling query)
  def eligible(m: int): bool = and {
    db.rows.keys().contains(m),
    isNull(db.rows.get(m).sendTime),
    val exp = db.rows.get(m).procExp
    isNull(exp) or exp <= now,
  }

  // Whether a comes before b in (CreateTime, ID) order
  def older(a: int, b: int): bool = {
    val ca = db.rows.get(a).createTime
    val cb = db.rows.get(b).createTime
    ca < cb or (ca == cb and a < b)
  }

  // Union over shards of ORDER BY CreateTime, ID LIMIT BATCH
  def candidates: Set[int] = {
    val el = MSGS.filter(m => eligible(m))
    el.filter(m => el.filter(o => SHARD_OF.get(o) == SHARD_OF.get(m) and older(o, m)).size() < BATCH)
  }

  // ---- Initial state ----
  action init = all {
    now' = 0,
    db' = { rows: Map(), deleted: Set(), business: Set() },
    fast' = MSGS.mapBy(_ => "none"),
    relays' = RELAYS.mapBy(_ => IDLE_RELAY),
    pubsub' = { accepted: Set(), copies: MSGS.mapBy(_ => 0) },
    consumer' = { handling: MSGS.mapBy(_ => 0), processed: Set(), effects: MSGS.mapBy(_ => 0) },
  }

  // ---- Producer ----

  // Commit the business update and the outbox row (SendTime=NULL) in one transaction
  action produce(m: int): bool = all {
    fast.get(m) == "none",
    db' = { ...db,
      rows: db.rows.put(m, { createTime: now, sendTime: NULL, procExp: NULL }),
      business: db.business.union(Set(m)),
    },
    fast' = fast.set(m, "committed"),
    now' = now, relays' = relays, pubsub' = pubsub, consumer' = consumer,
  }

  // fast-path: publish right after commit. On failure (or process stop) the row is left to the fallback
  action fastPublishWith(m: int, ok: bool): bool = all {
    fast.get(m) == "committed",
    if (ok and not(POISON.contains(m))) all {
      pubsub' = pubsub.published(m),
      fast' = fast.set(m, "published"),
    } else all {
      pubsub' = pubsub,
      fast' = fast.set(m, "done"),
    },
    now' = now, db' = db, relays' = relays, consumer' = consumer,
  }

  // fast-path: set SendTime after the publish ack (single mutation; no-op if the row is gone)
  action fastMarkSentWith(m: int, ok: bool): bool = all {
    fast.get(m) == "published",
    fast' = fast.set(m, "done"),
    db' = if (ok and db.rows.keys().contains(m))
            { ...db, rows: db.rows.setBy(m, r => { ...r, sendTime: now }) }
          else db,
    now' = now, relays' = relays, pubsub' = pubsub, consumer' = consumer,
  }

  // ---- Message Relay ----

  // Started by Cloud Scheduler: read candidates for every shard at once (single-use read-only Tx)
  action relayStart(r: str): bool = all {
    relays.get(r) == IDLE_RELAY,
    relays' = relays.set(r, { ...IDLE_RELAY, pending: candidates }),
    now' = now, db' = db, fast' = fast, pubsub' = pubsub, consumer' = consumer,
  }

  // Re-read one candidate FOR UPDATE; if still eligible, set ProcessingExpirationTime and commit
  action relayClaim(r: str, m: int): bool = all {
    relays.get(r).pending.contains(m),
    if (eligible(m)) all {
      db' = { ...db, rows: db.rows.setBy(m, row => { ...row, procExp: now + LEASE }) },
      relays' = relays.setBy(r, s => { ...s,
        pending: s.pending.exclude(Set(m)),
        claimed: s.claimed.union(Set(m)),
      }),
    } else all {
      db' = db,
      relays' = relays.setBy(r, s => { ...s, pending: s.pending.exclude(Set(m)) }),
    },
    now' = now, fast' = fast, pubsub' = pubsub, consumer' = consumer,
  }

  // Publish outside the Tx. A failed row keeps ProcessingExpirationTime and is skipped until it expires
  action relayPublishWith(r: str, m: int, ok: bool): bool = all {
    relays.get(r).claimed.contains(m),
    val success = ok and not(POISON.contains(m))
    all {
      pubsub' = if (success) pubsub.published(m) else pubsub,
      relays' = relays.setBy(r, s => { ...s,
        claimed: s.claimed.exclude(Set(m)),
        published: if (success) s.published.union(Set(m)) else s.published,
      }),
    },
    now' = now, db' = db, fast' = fast, consumer' = consumer,
  }

  // Set SendTime and clear ProcessingExpirationTime (single mutation outside the Tx)
  action relayMarkSentWith(r: str, m: int, ok: bool): bool = all {
    relays.get(r).published.contains(m),
    relays' = relays.setBy(r, s => { ...s, published: s.published.exclude(Set(m)) }),
    db' = if (ok and db.rows.keys().contains(m))
            { ...db, rows: db.rows.setBy(m, row => { ...row, sendTime: now, procExp: NULL }) }
          else db,
    now' = now, fast' = fast, pubsub' = pubsub, consumer' = consumer,
  }

  // The run crashes midway. Any ProcessingExpirationTime it set is reclaimed on expiry
  action relayCrash(r: str): bool = all {
    relays.get(r) != IDLE_RELAY,
    relays' = relays.set(r, IDLE_RELAY),
    now' = now, db' = db, fast' = fast, pubsub' = pubsub, consumer' = consumer,
  }

  // ---- Operations and TTL ----

  // TTL: delete rows whose SendTime is at least TTL old (unsent rows are never deleted)
  action ttlDelete(m: int): bool = all {
    db.rows.keys().contains(m),
    val t = db.rows.get(m).sendTime
    not(isNull(t)) and now - t >= TTL,
    db' = { ...db,
      rows: db.rows.keys().exclude(Set(m)).mapBy(k => db.rows.get(k)),
      deleted: db.deleted.union(Set(m)),
    },
    now' = now, fast' = fast, relays' = relays, pubsub' = pubsub, consumer' = consumer,
  }

  // Manual resend: reset SendTime and ProcessingExpirationTime to NULL
  action manualResend(m: int): bool = all {
    db.rows.keys().contains(m),
    not(isNull(db.rows.get(m).sendTime)),
    db' = { ...db, rows: db.rows.setBy(m, row => { ...row, sendTime: NULL, procExp: NULL }) },
    now' = now, fast' = fast, relays' = relays, pubsub' = pubsub, consumer' = consumer,
  }

  // ---- Consumer (push subscription) ----

  // Receive a delivery. Ack without work if already processed. With ATOMIC_CONSUMER, check and apply in one step
  action receive(m: int): bool = all {
    pubsub.copies.get(m) > 0,
    pubsub' = { ...pubsub, copies: pubsub.copies.setBy(m, c => c - 1) },
    consumer' =
      if (consumer.processed.contains(m)) consumer
      else if (ATOMIC_CONSUMER) { ...consumer,
        processed: consumer.processed.union(Set(m)),
        effects: consumer.effects.setBy(m, e => e + 1),
      }
      else { ...consumer, handling: consumer.handling.setBy(m, h => h + 1) },
    now' = now, db' = db, fast' = fast, relays' = relays,
  }

  // A handler that passed the idempotency check applies the effect, records it as processed, and acks
  action finish(m: int): bool = all {
    consumer.handling.get(m) > 0,
    consumer' = {
      handling: consumer.handling.setBy(m, h => h - 1),
      processed: consumer.processed.union(Set(m)),
      effects: consumer.effects.setBy(m, e => e + 1),
    },
    now' = now, db' = db, fast' = fast, relays' = relays, pubsub' = pubsub,
  }

  // The handler fails and nacks. Pub/Sub redelivers
  action nack(m: int): bool = all {
    consumer.handling.get(m) > 0,
    consumer' = { ...consumer, handling: consumer.handling.setBy(m, h => h - 1) },
    pubsub' = { ...pubsub, copies: pubsub.copies.setBy(m, c => c + 1) },
    now' = now, db' = db, fast' = fast, relays' = relays,
  }

  // ---- Time ----
  action tick: bool = all {
    now < MAX_TIME,
    now' = now + 1,
    db' = db, fast' = fast, relays' = relays, pubsub' = pubsub, consumer' = consumer,
  }

  action step = {
    nondet m = MSGS.oneOf()
    nondet r = RELAYS.oneOf()
    nondet ok = Set(true, false).oneOf()
    any {
      produce(m),
      fastPublishWith(m, ok),
      fastMarkSentWith(m, ok),
      relayStart(r),
      relayClaim(r, m),
      relayPublishWith(r, m, ok),
      relayMarkSentWith(r, m, ok),
      relayCrash(r),
      ttlDelete(m),
      manualResend(m),
      receive(m),
      finish(m),
      nack(m),
      tick,
    }
  }

  // ---- Properties ----

  // A business update and its outbox row always exist together (the row is missing only after TTL deletion)
  val atomicCommit = db.business == db.rows.keys().union(db.deleted)

  // A row with SendTime set was accepted by Pub/Sub at least once
  val sentImpliesAccepted =
    db.rows.keys().forall(m => not(isNull(db.rows.get(m).sendTime)) implies pubsub.accepted.contains(m))

  // Every committed message is still in the outbox (so it can be resent) or was accepted
  val noLostMessage =
    db.business.forall(m => db.rows.keys().contains(m) or pubsub.accepted.contains(m))

  // Even with duplicate delivery, each message's side effect is applied at most once
  val effectsAtMostOnce = MSGS.forall(m => consumer.effects.get(m) <= 1)

  val safety = and { atomicCommit, sentImpliesAccepted, noLostMessage, effectsAtMostOnce }

  // ---- Witnesses (check that exploration reaches interesting states) ----

  // A duplicate delivery is pending for an already processed message
  val duplicateDelivery = MSGS.exists(m => consumer.processed.contains(m) and pubsub.copies.get(m) > 0)

  // A sent row was deleted by TTL
  val ttlDeleted = db.deleted.size() > 0

  // Every message was committed and processed by the consumer
  val allProcessed = db.business == MSGS and consumer.processed == MSGS
}

// Consumer whose idempotency check and effect are atomic (e.g. the processed ID is written in the same Tx as the effect)
module outboxAtomic {
  import outbox(
    MSGS = Set(1, 2, 3),
    RELAYS = Set("r1", "r2"),
    SHARD_OF = Map(1 -> 0, 2 -> 1, 3 -> 0),
    POISON = Set(),
    BATCH = 1,
    LEASE = 2,
    TTL = 3,
    MAX_TIME = 8,
    ATOMIC_CONSUMER = true,
  ).*
}

// Consumer where "check processed -> apply -> record" is not atomic
module outboxNonAtomic {
  import outbox(
    MSGS = Set(1, 2, 3),
    RELAYS = Set("r1", "r2"),
    SHARD_OF = Map(1 -> 0, 2 -> 1, 3 -> 0),
    POISON = Set(),
    BATCH = 1,
    LEASE = 2,
    TTL = 3,
    MAX_TIME = 8,
    ATOMIC_CONSUMER = false,
  ).*
}

// Known limitation from the design: batch-limit permanently failing rows in a shard block the rows behind them
module outboxStarvation {
  import outbox(
    MSGS = Set(1, 2),
    RELAYS = Set("r1"),
    SHARD_OF = Map(1 -> 0, 2 -> 0),
    POISON = Set(1),
    BATCH = 1,
    LEASE = 1,
    TTL = 3,
    MAX_TIME = 20,
    ATOMIC_CONSUMER = true,
  ).*

  // One Scheduler run: claim and publish the candidate, then wait at least LEASE until the next run
  action scheduledRun = all {
    relayStart("r1"),
  }.then(relayClaim("r1", 1)).then(relayPublishWith("r1", 1, true)).then(tick)

  // Commit 1 (poison) and 2 at the same time; the fast-path for 2 fails
  run starvationTest =
    init
      .then(produce(1))
      .then(produce(2))
      .then(fastPublishWith(1, true))
      .then(fastPublishWith(2, false))
      .then(tick)
      .then(3.reps(_ => scheduledRun))
      .expect(not(pubsub.accepted.contains(2)) and candidates == Set(1))

  // If runs are closer together than LEASE, 2 becomes a candidate while 1 is being processed
  run shortIntervalTest =
    init
      .then(produce(1))
      .then(produce(2))
      .then(fastPublishWith(1, true))
      .then(fastPublishWith(2, false))
      .then(tick)
      .then(relayStart("r1"))
      .then(relayClaim("r1", 1))
      .then(relayPublishWith("r1", 1, true))
      .then(relayStart("r1"))
      .expect(relays.get("r1").pending == Set(2))
}
