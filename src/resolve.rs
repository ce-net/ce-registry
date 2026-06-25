//! The resolve filter-then-rank pipeline (design §4.4) — the pure core.
//!
//! Resolution is: discover providers (the DHT), read their health (the `HealthBook`), **filter** by
//! the health contract (Ready + fresh + version-matched), **then** rank by locate's atlas/trust/
//! recency signals intersected with self-reported load. This module is the deterministic core of
//! that pipeline: it takes an already-fetched set of candidate [`Instance`]s plus their health and
//! produces the filtered, ranked [`Resolved`] list — no I/O, fully testable over synthetic fixtures
//! (design §8). [`Registry`](crate::Registry) supplies the live `find_service`/`atlas`/book inputs.

use ce_rs::locate::Instance;

use crate::health::{Health, HealthBook};
use crate::version::VersionReq;

/// A resolved, resolvable instance: locate's ranked atlas [`Instance`] plus the application
/// [`Health`] it published (design §3.1). Resolution returns these.
#[derive(Clone, Debug)]
pub struct Resolved {
    /// The locate signals (NodeId, score, capacity, tags, fault domain).
    pub instance: Instance,
    /// The health record the instance published.
    pub health: Health,
}

impl Resolved {
    /// The instance's NodeId hex.
    pub fn node_id(&self) -> &str {
        &self.instance.node_id
    }
}

/// Why a candidate was dropped during filtering — surfaced so the resolver can distinguish
/// `NotFound` (nobody advertises) from `Unavailable` (advertised but none healthy) from
/// `FailedPrecondition` (advertised + healthy but none version-matched). Mirrors design §4.4.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// Advertised on the DHT but has no health record yet (warming / unknown).
    NoHealth,
    /// Health record present but phase is not Ready (Starting/Draining/Unhealthy).
    NotReady,
    /// Health record present and Ready but the last report is too old (missed heartbeats).
    Stale,
    /// Live (Ready + fresh) but its version does not satisfy the requirement.
    VersionMismatch,
}

/// The aggregate outcome of filtering a candidate set: the kept (resolvable, version-matched)
/// instances and a tally of *why* the rest were dropped. The tally is what lets the resolver pick
/// the right `Status` (design §4.4): any kept ⇒ rank and return; else if any was version-mismatched
/// ⇒ `FailedPrecondition`; else if any was advertised-but-unhealthy ⇒ `Unavailable`; else
/// `NotFound`.
#[derive(Debug, Default)]
pub struct FilterOutcome {
    /// Instances that passed every gate (Ready, fresh, version-matched), still in input order.
    pub kept: Vec<Resolved>,
    /// Count of each drop reason, for the NotFound/Unavailable/FailedPrecondition decision.
    pub no_health: usize,
    pub not_ready: usize,
    pub stale: usize,
    pub version_mismatch: usize,
}

impl FilterOutcome {
    /// Total candidates that were advertised at all (kept + every drop). Zero ⇒ nobody advertises.
    pub fn advertised(&self) -> usize {
        self.kept.len() + self.no_health + self.not_ready + self.stale + self.version_mismatch
    }

    /// Was any candidate live (Ready + fresh) — whether or not it version-matched? Distinguishes
    /// "advertised but all unhealthy" (`Unavailable`) from "healthy but wrong version"
    /// (`FailedPrecondition`).
    pub fn any_live(&self) -> bool {
        !self.kept.is_empty() || self.version_mismatch > 0
    }
}

/// Filter a candidate set against the health contract and a version requirement (design §4.4 steps
/// 2–5). Each candidate is an [`Instance`] (its locate/atlas signals) paired with the book; this
/// joins them, applies Phase/freshness/version gates, and tallies drop reasons.
///
/// * `now_ms` — the reader's current wall clock (ms).
/// * `stale_ms` — the freshness window (`k * refresh`); a record older than this is `Stale`.
///
/// The result preserves input order among `kept` (ranking is a separate step, [`rank`]).
pub fn filter(
    candidates: &[Instance],
    book: &HealthBook,
    req: &VersionReq,
    now_ms: u64,
    stale_ms: u64,
) -> FilterOutcome {
    let mut out = FilterOutcome::default();
    for inst in candidates {
        let Some(health) = book.get(&inst.node_id) else {
            out.no_health += 1; // advertised but no health record -> warming/unknown
            continue;
        };
        if !health.phase.is_resolvable() {
            out.not_ready += 1;
            continue;
        }
        if !health.is_fresh(now_ms, stale_ms) {
            out.stale += 1;
            continue;
        }
        if !req.matches(&health.version) {
            out.version_mismatch += 1;
            continue;
        }
        out.kept.push(Resolved { instance: inst.clone(), health: health.clone() });
    }
    out
}

/// Rank the kept candidates best-first (design §4.4 step 6). locate already scored each `Instance`
/// by trust/capacity/recency/beacon; the registry **intersects** that with the self-reported
/// `Health.load` (lower load is better), so a Ready-but-saturated instance ranks below a Ready-and-
/// idle one. The combined key is `instance.score - load_penalty`, a stable, deterministic sort.
///
/// `load_weight` scales how much self-reported load can move an instance (design notes load is a
/// *hint*, not a proof — keep its weight modest so trust still dominates). The sort is stable, so
/// equal keys preserve locate's original order (which already broke ties via the beacon).
pub fn rank(mut kept: Vec<Resolved>, load_weight: f64) -> Vec<Resolved> {
    kept.sort_by(|a, b| {
        let ka = effective_score(a, load_weight);
        let kb = effective_score(b, load_weight);
        kb.partial_cmp(&ka).unwrap_or(std::cmp::Ordering::Equal)
    });
    kept
}

/// The combined ranking key for a resolved instance: locate's composite `score` minus a load
/// penalty proportional to the self-reported saturation. Higher is better.
fn effective_score(r: &Resolved, load_weight: f64) -> f64 {
    let load = r.health.load.clamp(0.0, 1.0) as f64;
    r.instance.score - load_weight * load
}

/// The default weight for self-reported load in [`rank`]. Modest, so on-chain trust (locate's
/// largest component) still dominates the choice; load only breaks near-ties between equally
/// trusted instances. Self-reported load is a hint, not a proof (design §5).
pub const DEFAULT_LOAD_WEIGHT: f64 = 0.1;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::Phase;
    use crate::version::SemVer;
    use std::collections::BTreeMap;

    fn inst(id: &str, score: f64, domain: Option<&str>) -> Instance {
        Instance {
            node_id: id.into(),
            score,
            cores: 4,
            mem_mb: 4096,
            tags: domain.map(|d| vec![d.to_string()]).unwrap_or_default(),
            last_seen_secs: 0,
            fault_domain: domain.map(|d| d.to_string()),
        }
    }

    fn health(ver: SemVer, phase: Phase, load: f32, reported_at_ms: u64) -> Health {
        Health {
            version: ver,
            phase,
            load,
            epoch: 1,
            reported_at_ms,
            fault_domain: None,
            meta: BTreeMap::new(),
        }
    }

    fn book(rows: &[(&str, Health)]) -> HealthBook {
        use ce_coord::MergeMachine;
        let mut b = HealthBook::default();
        for (id, h) in rows {
            b.apply(crate::health::HealthOp { writer: id.to_string(), health: h.clone() });
        }
        b
    }

    const NOW: u64 = 100_000;
    const WINDOW: u64 = 30_000;

    #[test]
    fn filter_excludes_non_ready_stale_and_mismatch() {
        let candidates = vec![
            inst("ready_match", 0.9, None),
            inst("starting", 0.8, None),
            inst("draining", 0.7, None),
            inst("unhealthy", 0.6, None),
            inst("stale", 0.95, None),
            inst("wrong_ver", 0.99, None),
            inst("no_health", 0.5, None),
        ];
        let v1 = SemVer::new(1, 2, 0);
        let b = book(&[
            ("ready_match", health(v1, Phase::Ready, 0.1, 99_000)),
            ("starting", health(v1, Phase::Starting, 0.1, 99_000)),
            ("draining", health(v1, Phase::Draining, 0.1, 99_000)),
            ("unhealthy", health(v1, Phase::Unhealthy, 0.1, 99_000)),
            ("stale", health(v1, Phase::Ready, 0.1, 50_000)),
            ("wrong_ver", health(SemVer::new(2, 0, 0), Phase::Ready, 0.1, 99_000)),
            // "no_health" deliberately absent from the book
        ]);
        let req = VersionReq::Caret(1, 0, 0);
        let out = filter(&candidates, &b, &req, NOW, WINDOW);

        let kept: Vec<_> = out.kept.iter().map(|r| r.node_id().to_string()).collect();
        assert_eq!(kept, vec!["ready_match".to_string()]);
        assert_eq!(out.no_health, 1);
        assert_eq!(out.not_ready, 3); // starting + draining + unhealthy
        assert_eq!(out.stale, 1);
        assert_eq!(out.version_mismatch, 1);
        assert_eq!(out.advertised(), 7);
        assert!(out.any_live());
    }

    #[test]
    fn filter_distinguishes_unavailable_from_failed_precondition() {
        let v1 = SemVer::new(1, 0, 0);
        // all advertised, all Ready+fresh, but none version-matches a ^2 req -> any_live true,
        // kept empty -> FailedPrecondition territory.
        let candidates = vec![inst("a", 0.9, None), inst("b", 0.8, None)];
        let b = book(&[
            ("a", health(v1, Phase::Ready, 0.1, 99_000)),
            ("b", health(v1, Phase::Ready, 0.1, 99_000)),
        ]);
        let out = filter(&candidates, &b, &VersionReq::Caret(2, 0, 0), NOW, WINDOW);
        assert!(out.kept.is_empty());
        assert!(out.any_live(), "version-mismatch means some were live -> FailedPrecondition");
        assert_eq!(out.version_mismatch, 2);

        // all advertised but all Draining -> not live -> Unavailable territory.
        let b2 = book(&[
            ("a", health(v1, Phase::Draining, 0.1, 99_000)),
            ("b", health(v1, Phase::Draining, 0.1, 99_000)),
        ]);
        let out2 = filter(&candidates, &b2, &VersionReq::Caret(1, 0, 0), NOW, WINDOW);
        assert!(out2.kept.is_empty());
        assert!(!out2.any_live(), "all unhealthy -> Unavailable");
        assert_eq!(out2.advertised(), 2);
    }

    #[test]
    fn rank_orders_by_score_then_penalizes_load() {
        let v1 = SemVer::new(1, 0, 0);
        // Two equally-scored instances; the less-loaded one ranks first.
        let kept = vec![
            Resolved { instance: inst("loaded", 0.8, None), health: health(v1, Phase::Ready, 0.9, NOW) },
            Resolved { instance: inst("idle", 0.8, None), health: health(v1, Phase::Ready, 0.0, NOW) },
        ];
        let ranked = rank(kept, DEFAULT_LOAD_WEIGHT);
        assert_eq!(ranked[0].node_id(), "idle");
        assert_eq!(ranked[1].node_id(), "loaded");
    }

    #[test]
    fn rank_respects_locate_score_when_all_ready() {
        // design §8: ranking order matches locate when all Ready (and equal load).
        let v1 = SemVer::new(1, 0, 0);
        let kept = vec![
            Resolved { instance: inst("low", 0.3, None), health: health(v1, Phase::Ready, 0.0, NOW) },
            Resolved { instance: inst("high", 0.9, None), health: health(v1, Phase::Ready, 0.0, NOW) },
            Resolved { instance: inst("mid", 0.6, None), health: health(v1, Phase::Ready, 0.0, NOW) },
        ];
        let ranked = rank(kept, DEFAULT_LOAD_WEIGHT);
        let order: Vec<_> = ranked.iter().map(|r| r.node_id().to_string()).collect();
        assert_eq!(order, vec!["high".to_string(), "mid".to_string(), "low".to_string()]);
    }

    #[test]
    fn empty_candidates_is_not_found_shape() {
        let out = filter(&[], &HealthBook::default(), &VersionReq::Any, NOW, WINDOW);
        assert_eq!(out.advertised(), 0);
        assert!(!out.any_live());
        assert!(out.kept.is_empty());
    }
}
