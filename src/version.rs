//! Semantic versioning for the registry — `SemVer`, `VersionReq`, and the `matches` core.
//!
//! This is the **binding correctness core** (design §3.1, §8): a dependent declares a
//! [`VersionReq`] (`^1`, `~1.2`, `>=2.1`, `=1.4.0`, `*`) and the resolver keeps only instances
//! whose advertised [`SemVer`] satisfies it. It is pure, deterministic, dependency-free logic and
//! is fully unit/property-tested below — nothing here needs a mesh.
//!
//! ## The major axis vs. the minor/patch axis (design §4.1)
//!
//! The **major** version is the DHT-key axis: a service is advertised under
//! `"<ns>/<name>@<major>"`, so a major bump is a breaking change and a *distinct* service key (the
//! semver contract). [`VersionReq::major_floor`] / [`VersionReq::compatible_major`] expose which
//! major(s) a requirement can be satisfied by, so the resolver knows which DHT key(s) to query.
//! The **minor/patch** axis is matched client-side here, against the [`SemVer`] carried in each
//! instance's health record.

use serde::{Deserialize, Serialize};

/// A `major.minor.patch` semantic version an instance advertises (design §3.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SemVer {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl SemVer {
    /// Construct a version.
    pub const fn new(major: u32, minor: u32, patch: u32) -> Self {
        SemVer { major, minor, patch }
    }
}

impl PartialOrd for SemVer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SemVer {
    /// Precedence is `major`, then `minor`, then `patch` — the standard semver ordering.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.major, self.minor, self.patch).cmp(&(other.major, other.minor, other.patch))
    }
}

impl std::fmt::Display for SemVer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// A version constraint a dependent declares against an instance's [`SemVer`] (design §3.1).
///
/// The matching rules follow the standard Cargo/npm caret/tilde semantics:
/// * [`Caret`](VersionReq::Caret)`(a,b,c)` — `>= a.b.c` and `< (a+1).0.0` (compatible-within-major;
///   the common `^1` / `^1.2.3` case). For `a == 0` the caret narrows to the first non-zero
///   component per the semver spec (`^0.2.3` ⇒ `>=0.2.3, <0.3.0`; `^0.0.3` ⇒ `>=0.0.3, <0.0.4`).
/// * [`Tilde`](VersionReq::Tilde)`(a,b,c)` — `>= a.b.c` and `< a.(b+1).0` (patch-level changes).
/// * [`Gte`](VersionReq::Gte)`(v)` — any version `>= v`, with no upper bound.
/// * [`Exact`](VersionReq::Exact)`(v)` — exactly `v`.
/// * [`Any`](VersionReq::Any) — `*`, any version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum VersionReq {
    Caret(u32, u32, u32),
    Tilde(u32, u32, u32),
    Gte(SemVer),
    Exact(SemVer),
    Any,
}

impl VersionReq {
    /// Does `v` satisfy this requirement? The pure matcher (design §8: the binding-correctness core).
    pub fn matches(&self, v: &SemVer) -> bool {
        match self {
            VersionReq::Any => true,
            VersionReq::Exact(want) => v == want,
            VersionReq::Gte(floor) => v >= floor,
            VersionReq::Tilde(a, b, c) => {
                let floor = SemVer::new(*a, *b, *c);
                // < a.(b+1).0  — patch-level (and the given patch floor) within the fixed minor.
                let ceil = SemVer::new(*a, b.saturating_add(1), 0);
                v >= &floor && v < &ceil
            }
            VersionReq::Caret(a, b, c) => {
                let floor = SemVer::new(*a, *b, *c);
                let ceil = caret_ceiling(*a, *b, *c);
                v >= &floor && v < &ceil
            }
        }
    }

    /// The lowest major version that could possibly satisfy this requirement. Used to choose which
    /// `"<ns>/<name>@<major>"` DHT key(s) to query (design §4.1, §4.4). `Any`/`Gte(0…)` floor at 0.
    pub fn major_floor(&self) -> u32 {
        match self {
            VersionReq::Any => 0,
            VersionReq::Exact(v) | VersionReq::Gte(v) => v.major,
            VersionReq::Caret(a, _, _) | VersionReq::Tilde(a, _, _) => *a,
        }
    }

    /// Could an instance advertising major `m` possibly satisfy this requirement? This is the
    /// coarse DHT-key filter: `Caret`/`Tilde`/`Exact` pin exactly one major; `Gte` admits every
    /// major `>=` its floor; `Any` admits all. A `true` here means "query that major's DHT key";
    /// the fine minor/patch gate is still [`matches`](Self::matches).
    pub fn compatible_major(&self, m: u32) -> bool {
        match self {
            VersionReq::Any => true,
            VersionReq::Gte(v) => m >= v.major,
            VersionReq::Exact(v) => m == v.major,
            VersionReq::Caret(a, _, _) | VersionReq::Tilde(a, _, _) => m == *a,
        }
    }

    /// The exact set of major versions whose DHT keys a resolver should query for this requirement,
    /// bounded by `seen_majors` (the majors actually known to exist for this service name, e.g. from
    /// a small probe range). For an unbounded `Gte`/`Any` the caller supplies the candidate majors;
    /// for a pinned requirement this is a single-element set. Deterministic and sorted.
    pub fn query_majors(&self, seen_majors: &[u32]) -> Vec<u32> {
        match self {
            VersionReq::Caret(a, _, _) | VersionReq::Tilde(a, _, _) => vec![*a],
            VersionReq::Exact(v) => vec![v.major],
            VersionReq::Gte(_) | VersionReq::Any => {
                let mut majors: Vec<u32> =
                    seen_majors.iter().copied().filter(|m| self.compatible_major(*m)).collect();
                majors.sort_unstable();
                majors.dedup();
                majors
            }
        }
    }
}

impl std::fmt::Display for VersionReq {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VersionReq::Any => write!(f, "*"),
            VersionReq::Exact(v) => write!(f, "={v}"),
            VersionReq::Gte(v) => write!(f, ">={v}"),
            VersionReq::Tilde(a, b, c) => write!(f, "~{a}.{b}.{c}"),
            VersionReq::Caret(a, b, c) => write!(f, "^{a}.{b}.{c}"),
        }
    }
}

/// The exclusive upper bound of a caret requirement, honoring the leading-zero rule of semver:
/// `^a.b.c` is `< (a+1).0.0` when `a>0`, `< 0.(b+1).0` when `a==0 && b>0`, and `< 0.0.(c+1)` when
/// `a==0 && b==0` (a `0.0.x` release is its own incompatible unit).
fn caret_ceiling(a: u32, b: u32, c: u32) -> SemVer {
    if a > 0 {
        SemVer::new(a.saturating_add(1), 0, 0)
    } else if b > 0 {
        SemVer::new(0, b.saturating_add(1), 0)
    } else {
        SemVer::new(0, 0, c.saturating_add(1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(a: u32, b: u32, c: u32) -> SemVer {
        SemVer::new(a, b, c)
    }

    #[test]
    fn semver_ordering() {
        assert!(v(1, 0, 0) < v(1, 0, 1));
        assert!(v(1, 0, 1) < v(1, 1, 0));
        assert!(v(1, 9, 9) < v(2, 0, 0));
        assert_eq!(v(1, 2, 3), v(1, 2, 3));
    }

    #[test]
    fn caret_basic_within_major() {
        let r = VersionReq::Caret(1, 2, 0);
        // design §8: ^1.2.0 matches 1.x.y >= 1.2.0, rejects 2.0.0 and 1.1.9.
        assert!(r.matches(&v(1, 2, 0)));
        assert!(r.matches(&v(1, 2, 5)));
        assert!(r.matches(&v(1, 9, 9)));
        assert!(!r.matches(&v(1, 1, 9)));
        assert!(!r.matches(&v(2, 0, 0)));
        assert!(!r.matches(&v(0, 9, 9)));
    }

    #[test]
    fn caret_leading_zero_rules() {
        // ^0.2.3 => >=0.2.3, <0.3.0
        let r = VersionReq::Caret(0, 2, 3);
        assert!(r.matches(&v(0, 2, 3)));
        assert!(r.matches(&v(0, 2, 9)));
        assert!(!r.matches(&v(0, 3, 0)));
        assert!(!r.matches(&v(0, 2, 2)));
        // ^0.0.3 => >=0.0.3, <0.0.4 (exactly that patch)
        let r = VersionReq::Caret(0, 0, 3);
        assert!(r.matches(&v(0, 0, 3)));
        assert!(!r.matches(&v(0, 0, 4)));
        assert!(!r.matches(&v(0, 1, 0)));
    }

    #[test]
    fn tilde_patch_within_minor() {
        let r = VersionReq::Tilde(1, 2, 0);
        assert!(r.matches(&v(1, 2, 0)));
        assert!(r.matches(&v(1, 2, 9)));
        assert!(!r.matches(&v(1, 3, 0)));
        assert!(!r.matches(&v(1, 1, 9)));
        assert!(!r.matches(&v(2, 2, 0)));
    }

    #[test]
    fn gte_and_exact_and_any() {
        let r = VersionReq::Gte(v(2, 1, 0));
        assert!(r.matches(&v(2, 1, 0)));
        assert!(r.matches(&v(9, 9, 9)));
        assert!(!r.matches(&v(2, 0, 9)));

        let r = VersionReq::Exact(v(1, 4, 0));
        assert!(r.matches(&v(1, 4, 0)));
        assert!(!r.matches(&v(1, 4, 1)));

        assert!(VersionReq::Any.matches(&v(0, 0, 0)));
        assert!(VersionReq::Any.matches(&v(42, 7, 1)));
    }

    #[test]
    fn major_floor_and_compatible_major() {
        assert_eq!(VersionReq::Caret(2, 1, 0).major_floor(), 2);
        assert_eq!(VersionReq::Tilde(3, 0, 0).major_floor(), 3);
        assert_eq!(VersionReq::Exact(v(4, 0, 0)).major_floor(), 4);
        assert_eq!(VersionReq::Gte(v(2, 0, 0)).major_floor(), 2);
        assert_eq!(VersionReq::Any.major_floor(), 0);

        assert!(VersionReq::Caret(2, 0, 0).compatible_major(2));
        assert!(!VersionReq::Caret(2, 0, 0).compatible_major(1));
        assert!(!VersionReq::Caret(2, 0, 0).compatible_major(3));
        assert!(VersionReq::Gte(v(2, 0, 0)).compatible_major(5));
        assert!(!VersionReq::Gte(v(2, 0, 0)).compatible_major(1));
        assert!(VersionReq::Any.compatible_major(99));
    }

    #[test]
    fn query_majors_selects_dht_keys() {
        let seen = [1u32, 2, 3, 4];
        assert_eq!(VersionReq::Caret(2, 0, 0).query_majors(&seen), vec![2]);
        assert_eq!(VersionReq::Exact(v(3, 0, 0)).query_majors(&seen), vec![3]);
        assert_eq!(VersionReq::Gte(v(2, 0, 0)).query_majors(&seen), vec![2, 3, 4]);
        assert_eq!(VersionReq::Any.query_majors(&seen), vec![1, 2, 3, 4]);
        // pinned reqs ignore `seen` (they always query their one major)
        assert_eq!(VersionReq::Caret(9, 0, 0).query_majors(&seen), vec![9]);
    }

    #[test]
    fn display_roundtrips_shape() {
        assert_eq!(VersionReq::Caret(1, 2, 3).to_string(), "^1.2.3");
        assert_eq!(VersionReq::Tilde(1, 2, 0).to_string(), "~1.2.0");
        assert_eq!(VersionReq::Gte(v(2, 1, 0)).to_string(), ">=2.1.0");
        assert_eq!(VersionReq::Exact(v(1, 4, 0)).to_string(), "=1.4.0");
        assert_eq!(VersionReq::Any.to_string(), "*");
        assert_eq!(v(1, 2, 3).to_string(), "1.2.3");
    }

    // ---- property tests (design §8: caret/tilde/gte/exact/any against a SemVer table) ----
    use proptest::prelude::*;

    proptest! {
        /// A caret `^a.b.c` matches exactly `[a.b.c, ceiling)` and nothing else.
        #[test]
        fn prop_caret_is_a_half_open_interval(
            a in 0u32..5, b in 0u32..5, c in 0u32..5,
            ta in 0u32..6, tb in 0u32..8, tc in 0u32..8,
        ) {
            let r = VersionReq::Caret(a, b, c);
            let floor = SemVer::new(a, b, c);
            let ceil = caret_ceiling(a, b, c);
            let cand = SemVer::new(ta, tb, tc);
            let in_interval = cand >= floor && cand < ceil;
            prop_assert_eq!(r.matches(&cand), in_interval);
        }

        /// A tilde `~a.b.c` matches exactly `[a.b.c, a.(b+1).0)`.
        #[test]
        fn prop_tilde_is_patch_interval(
            a in 0u32..5, b in 0u32..5, c in 0u32..5,
            ta in 0u32..6, tb in 0u32..8, tc in 0u32..8,
        ) {
            let r = VersionReq::Tilde(a, b, c);
            let floor = SemVer::new(a, b, c);
            let ceil = SemVer::new(a, b.saturating_add(1), 0);
            let cand = SemVer::new(ta, tb, tc);
            prop_assert_eq!(r.matches(&cand), cand >= floor && cand < ceil);
        }

        /// Caret with a non-zero major never matches a different major (the DHT-key invariant).
        #[test]
        fn prop_caret_major_pins_dht_axis(
            a in 1u32..5, b in 0u32..5, c in 0u32..5,
            ma in 0u32..6, mb in 0u32..8, mc in 0u32..8,
        ) {
            let r = VersionReq::Caret(a, b, c);
            let cand = SemVer::new(ma, mb, mc);
            if r.matches(&cand) {
                prop_assert_eq!(cand.major, a, "a match must share the pinned major");
                prop_assert!(r.compatible_major(cand.major));
            }
        }

        /// `matches` is consistent with the coarse `compatible_major` gate: a full match implies a
        /// major match, so the DHT-key prefilter never excludes a real candidate.
        #[test]
        fn prop_matches_implies_compatible_major(
            req in any_versionreq(),
            ta in 0u32..6, tb in 0u32..8, tc in 0u32..8,
        ) {
            let cand = SemVer::new(ta, tb, tc);
            if req.matches(&cand) {
                prop_assert!(req.compatible_major(cand.major));
            }
        }
    }

    fn any_versionreq() -> impl Strategy<Value = VersionReq> {
        prop_oneof![
            (0u32..4, 0u32..4, 0u32..4).prop_map(|(a, b, c)| VersionReq::Caret(a, b, c)),
            (0u32..4, 0u32..4, 0u32..4).prop_map(|(a, b, c)| VersionReq::Tilde(a, b, c)),
            (0u32..4, 0u32..4, 0u32..4).prop_map(|(a, b, c)| VersionReq::Gte(SemVer::new(a, b, c))),
            (0u32..4, 0u32..4, 0u32..4).prop_map(|(a, b, c)| VersionReq::Exact(SemVer::new(a, b, c))),
            Just(VersionReq::Any),
        ]
    }
}
