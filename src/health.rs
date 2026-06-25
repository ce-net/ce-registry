//! The health/readiness contract and the multi-writer `HealthBook` CRDT (design §3.1, §4.2).
//!
//! Every registered instance publishes a [`Health`] record and refreshes it periodically. Readers
//! merge all writers' records into a [`HealthBook`] — a [`MergeMachine`](ce_coord::MergeMachine)
//! keyed by `(writer, epoch)` with **per-writer last-write-wins by epoch**. That makes concurrent
//! registrations/heartbeats from N independent instances commutative and idempotent (design §4.2,
//! §5): the book converges regardless of arrival order, with no central writer and no coordinator.
//!
//! `Health` mirrors the k8s readiness/liveness split without putting any of it in the node:
//! `phase`/`load` are app-reported; the registry treats them uniformly. The `epoch` field reuses
//! the chain's replay-proof Heartbeat pattern ("epochs strictly increase").

use std::collections::BTreeMap;

use ce_coord::MergeMachine;
use serde::{Deserialize, Serialize};

use crate::version::SemVer;

/// A registered instance's NodeId, as the hex string `ce-rs`/`ce-coord` use on the wire. The
/// `HealthBook` keys writers by this so it is the same identity `find_service`/`Merged` speak.
pub type InstanceId = String;

/// The lifecycle phase an instance self-reports (design §3.1). Only [`Phase::Ready`] instances are
/// resolvable; the others are excluded by the resolver's health filter (design §4.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Phase {
    /// Warming up (caches, schema migration, dependency bind). Advertised but not yet resolvable.
    Starting,
    /// Live and accepting work. The only resolvable phase.
    Ready,
    /// Gracefully shutting down; finish in-flight work but take no new resolves. Excluded.
    Draining,
    /// Self-reported unhealthy (failed a liveness probe). Excluded.
    Unhealthy,
}

impl Phase {
    /// Is an instance in this phase resolvable (i.e. handed to callers)? Only [`Phase::Ready`].
    pub fn is_resolvable(self) -> bool {
        matches!(self, Phase::Ready)
    }
}

/// The standard health/readiness record every registered instance publishes and refreshes
/// (design §3.1). This is the CRDT *value* the registry filters on before ranking.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Health {
    /// The exact `major.minor.patch` this instance runs. The major is also the DHT-key axis; the
    /// minor/patch are matched client-side against a [`VersionReq`](crate::VersionReq).
    pub version: SemVer,
    /// Lifecycle phase. Only [`Phase::Ready`] is resolvable.
    pub phase: Phase,
    /// Self-reported saturation in `0.0..=1.0` (07-telemetry SLI when available). Lower is better;
    /// the resolver intersects this with locate's capacity ranking.
    pub load: f32,
    /// Strictly-increasing per instance — the LWW ordinal and replay guard (design §4.2). A reader
    /// keeps only the highest-epoch record per writer; a stale or replayed older epoch is ignored.
    pub epoch: u64,
    /// The instance's wall-clock time (unix-ms, from `Ctx.hlc.0`) when this record was produced.
    /// Readers age an instance out when `now - reported_at_ms > k * refresh` (design §4.4, §5):
    /// readiness is decoupled from mere DHT reachability.
    pub reported_at_ms: u64,
    /// Region/zone/asn fault domain (reuses locate's `region:`/`zone:`/`asn:` tag convention) for
    /// redundancy spread in `resolve_many`.
    pub fault_domain: Option<String>,
    /// App extras the registry passes through verbatim — endpoint topic, shard range, etc.
    pub meta: BTreeMap<String, String>,
}

impl Health {
    /// Is this record fresh enough to trust, given `now_ms` and the staleness window `stale_ms`?
    /// A record older than the window is treated as dead-to-readers — the instance missed its
    /// heartbeats (design §4.4, §5: "staleness = missed epochs"). `now < reported_at` (clock skew)
    /// is treated as fresh.
    pub fn is_fresh(&self, now_ms: u64, stale_ms: u64) -> bool {
        now_ms.saturating_sub(self.reported_at_ms) <= stale_ms
    }

    /// Is this instance currently resolvable: [`Phase::Ready`] AND fresh? The two gates the
    /// resolver applies before version-matching and ranking (design §4.4).
    pub fn is_live(&self, now_ms: u64, stale_ms: u64) -> bool {
        self.phase.is_resolvable() && self.is_fresh(now_ms, stale_ms)
    }
}

/// The default staleness multiplier `k` (design §4.4/§5): an instance is dead-to-readers when no
/// fresh record has arrived within `k * refresh`. With the default `refresh = 10s`, `k = 3` gives
/// ≤ 30s crash detection.
pub const DEFAULT_STALENESS_K: u32 = 3;

/// Compute the staleness window in ms from a refresh interval and the multiplier `k`.
pub fn stale_ms(refresh_ms: u64, k: u32) -> u64 {
    refresh_ms.saturating_mul(k as u64)
}

/// One op into the health book: a writer publishing a fresh health record for *itself*.
///
/// Authenticity (design §6): a `Merged` op is signed by its writer, and the followed-writer check
/// means a reader only applies ops from the writer it follows — so an instance can never publish
/// health *as another instance*. The `(writer, epoch)` key reflects that: the writer is fixed by
/// the node-verified sender, never chosen by the op.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthOp {
    /// The publishing instance (its NodeId hex). Must equal the verified sender at the node.
    pub writer: InstanceId,
    /// The health record being published.
    pub health: Health,
}

/// The per-`(ns, name@major)` health book: a `NodeId -> Health` map with **per-writer LWW by
/// epoch** (design §4.2). It is a [`MergeMachine`](ce_coord::MergeMachine), so it rides
/// `ce-coord`'s leaderless multi-writer convergence: every instance is its own writer of its own
/// row; readers take the union; concurrent updates are commutative and idempotent.
#[derive(Default, Debug, Clone, PartialEq)]
pub struct HealthBook {
    map: BTreeMap<InstanceId, Health>,
}

impl HealthBook {
    /// The current health record for `instance`, if the book has one.
    pub fn get(&self, instance: &str) -> Option<&Health> {
        self.map.get(instance)
    }

    /// All `(instance, health)` rows currently in the book.
    pub fn entries(&self) -> impl Iterator<Item = (&InstanceId, &Health)> {
        self.map.iter()
    }

    /// Number of instances with a row.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// True if the book has no rows.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// The instances currently live (Ready + fresh) at `now_ms` under `stale_ms` (design §4.4).
    pub fn live_instances(&self, now_ms: u64, stale_ms: u64) -> Vec<(&InstanceId, &Health)> {
        self.map.iter().filter(|(_, h)| h.is_live(now_ms, stale_ms)).collect()
    }
}

impl MergeMachine for HealthBook {
    type Op = HealthOp;
    /// `(writer, epoch)` — a strict total order, never shared between distinct ops (design §4.2).
    /// Distinct epochs from one writer order; distinct writers are independent rows.
    type Key = (InstanceId, u64);

    fn key(op: &HealthOp) -> Self::Key {
        (op.writer.clone(), op.health.epoch)
    }

    /// Keep the highest-epoch [`Health`] per writer; ignore an equal-or-older epoch (idempotent,
    /// gap-tolerant — the LWW-by-epoch machine of design §4.2). Because `Merged` folds ops in
    /// ascending `(writer, epoch)` key order, this is order-independent: the final row for each
    /// writer is its latest epoch regardless of delivery order.
    fn apply(&mut self, op: HealthOp) {
        if let Some(prev) = self.map.get(&op.writer) {
            if prev.epoch >= op.health.epoch {
                return;
            }
        }
        self.map.insert(op.writer, op.health);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(epoch: u64, phase: Phase, reported_at_ms: u64) -> Health {
        Health {
            version: SemVer::new(1, 0, 0),
            phase,
            load: 0.1,
            epoch,
            reported_at_ms,
            fault_domain: None,
            meta: BTreeMap::new(),
        }
    }

    fn op(writer: &str, epoch: u64, phase: Phase) -> HealthOp {
        HealthOp { writer: writer.to_string(), health: h(epoch, phase, epoch * 1000) }
    }

    // The canonical Merged fold: dedup ops into a (writer,epoch)-ordered BTreeMap, then apply in
    // ascending key order from default. Mirrors ce_coord::merged::Shared::fold exactly.
    fn fold(ops: &[HealthOp]) -> HealthBook {
        let mut union: BTreeMap<(InstanceId, u64), HealthOp> = BTreeMap::new();
        for o in ops {
            union.insert(HealthBook::key(o), o.clone());
        }
        let mut book = HealthBook::default();
        for o in union.values() {
            book.apply(o.clone());
        }
        book
    }

    #[test]
    fn higher_epoch_wins_older_ignored() {
        let mut book = HealthBook::default();
        book.apply(op("n1", 5, Phase::Ready));
        book.apply(op("n1", 3, Phase::Starting)); // older epoch -> ignored
        assert_eq!(book.get("n1").unwrap().epoch, 5);
        assert_eq!(book.get("n1").unwrap().phase, Phase::Ready);
        book.apply(op("n1", 7, Phase::Draining)); // newer -> wins
        assert_eq!(book.get("n1").unwrap().epoch, 7);
        assert_eq!(book.get("n1").unwrap().phase, Phase::Draining);
    }

    #[test]
    fn equal_epoch_is_idempotent() {
        let mut book = HealthBook::default();
        book.apply(op("n1", 4, Phase::Ready));
        // a duplicate of the same epoch must not change anything
        book.apply(op("n1", 4, Phase::Unhealthy));
        assert_eq!(book.get("n1").unwrap().phase, Phase::Ready);
    }

    #[test]
    fn writers_are_independent_rows() {
        let mut book = HealthBook::default();
        book.apply(op("n1", 2, Phase::Ready));
        book.apply(op("n2", 1, Phase::Starting));
        assert_eq!(book.len(), 2);
        assert_eq!(book.get("n1").unwrap().epoch, 2);
        assert_eq!(book.get("n2").unwrap().epoch, 1);
    }

    #[test]
    fn fold_is_order_independent() {
        let ops = vec![
            op("n1", 1, Phase::Starting),
            op("n1", 3, Phase::Ready),
            op("n2", 2, Phase::Ready),
            op("n1", 2, Phase::Starting),
            op("n2", 5, Phase::Draining),
        ];
        let reference = fold(&ops);
        // n1 latest epoch 3 (Ready), n2 latest epoch 5 (Draining)
        assert_eq!(reference.get("n1").unwrap().epoch, 3);
        assert_eq!(reference.get("n1").unwrap().phase, Phase::Ready);
        assert_eq!(reference.get("n2").unwrap().epoch, 5);

        let mut reversed = ops.clone();
        reversed.reverse();
        assert_eq!(fold(&reversed), reference);
    }

    #[test]
    fn freshness_and_liveness_gates() {
        let now = 100_000u64;
        let window = 30_000u64;
        let fresh_ready = Health { reported_at_ms: 80_000, ..h(1, Phase::Ready, 0) };
        assert!(fresh_ready.is_fresh(now, window));
        assert!(fresh_ready.is_live(now, window));

        let stale = Health { reported_at_ms: 50_000, ..h(1, Phase::Ready, 0) };
        assert!(!stale.is_fresh(now, window));
        assert!(!stale.is_live(now, window));

        let starting = Health { reported_at_ms: 99_000, ..h(1, Phase::Starting, 0) };
        assert!(starting.is_fresh(now, window));
        assert!(!starting.is_live(now, window), "Starting is not resolvable");

        // clock skew: reported in the future is treated as fresh, not stale.
        let future = Health { reported_at_ms: 200_000, ..h(1, Phase::Ready, 0) };
        assert!(future.is_fresh(now, window));
    }

    #[test]
    fn stale_ms_helper() {
        assert_eq!(stale_ms(10_000, DEFAULT_STALENESS_K), 30_000);
        assert_eq!(stale_ms(5_000, 4), 20_000);
    }

    #[test]
    fn live_instances_filters_book() {
        let now = 100_000u64;
        let window = 30_000u64;
        let mut book = HealthBook::default();
        book.apply(HealthOp {
            writer: "ready".into(),
            health: Health { reported_at_ms: 99_000, ..h(1, Phase::Ready, 0) },
        });
        book.apply(HealthOp {
            writer: "draining".into(),
            health: Health { reported_at_ms: 99_000, ..h(1, Phase::Draining, 0) },
        });
        book.apply(HealthOp {
            writer: "stale".into(),
            health: Health { reported_at_ms: 10_000, ..h(1, Phase::Ready, 0) },
        });
        let live: Vec<_> = book.live_instances(now, window).into_iter().map(|(id, _)| id.clone()).collect();
        assert_eq!(live, vec!["ready".to_string()]);
    }

    // ---- property: convergence under any permutation/duplication (design §8, mirrors merged.rs) --
    use proptest::prelude::*;

    fn ops_from(raw: &[(u8, u64, u8)]) -> Vec<HealthOp> {
        // assign each (writer) a strictly-increasing epoch from its position so keys are unique.
        let mut per_writer: std::collections::HashMap<u8, u64> = std::collections::HashMap::new();
        raw.iter()
            .map(|(w, _, phase_sel)| {
                let e = per_writer.entry(*w).or_insert(0);
                *e += 1;
                let phase = match phase_sel % 4 {
                    0 => Phase::Starting,
                    1 => Phase::Ready,
                    2 => Phase::Draining,
                    _ => Phase::Unhealthy,
                };
                op(&format!("w{w}"), *e, phase)
            })
            .collect()
    }

    proptest! {
        #[test]
        fn prop_healthbook_converges_under_any_order(
            raw in proptest::collection::vec((0u8..4, 0u64..50, 0u8..4), 1..40),
            seed in any::<u64>(),
        ) {
            let ops = ops_from(&raw);
            let reference = fold(&ops);

            // reversed delivery
            let mut reversed = ops.clone();
            reversed.reverse();
            prop_assert_eq!(fold(&reversed), reference.clone());

            // LCG shuffle
            let mut shuffled = ops.clone();
            let mut s = seed | 1;
            for i in (1..shuffled.len()).rev() {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let j = (s >> 33) as usize % (i + 1);
                shuffled.swap(i, j);
            }
            prop_assert_eq!(fold(&shuffled), reference.clone());

            // duplicated + reversed appended -> idempotent
            let mut doubled = ops.clone();
            doubled.extend(ops.iter().rev().cloned());
            prop_assert_eq!(fold(&doubled), reference.clone());
        }

        /// The converged row for each writer is its highest epoch (the LWW guarantee).
        #[test]
        fn prop_healthbook_keeps_latest_epoch_per_writer(
            raw in proptest::collection::vec((0u8..4, 0u64..50, 0u8..4), 1..40),
        ) {
            let ops = ops_from(&raw);
            let book = fold(&ops);
            // For each writer, the max epoch seen must equal the book's stored epoch.
            let mut max_epoch: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
            for o in &ops {
                let e = max_epoch.entry(o.writer.clone()).or_insert(0);
                *e = (*e).max(o.health.epoch);
            }
            for (w, e) in max_epoch {
                prop_assert_eq!(book.get(&w).unwrap().epoch, e);
            }
        }
    }
}
