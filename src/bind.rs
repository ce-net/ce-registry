//! The dependency resolver — `Dep`, `Bindings`, `BindError`, and the bind decision core
//! (design §3.1, §4.5).
//!
//! `bind` is the literal "dependencies" mechanism: an app declares `needs storage@^1, db@^2` and
//! must resolve every required dep to a live, version-matched instance before it starts, failing
//! fast and legibly if any is unsatisfiable. This module holds the declaration types and the
//! deterministic *decision* logic — "given each dep's current resolve outcome, are we done, still
//! waiting, or permanently failed?" — separated from the async retry/await loop in
//! [`Registry`](crate::Registry) so it is unit-testable with a fake clock and fixed outcomes
//! (design §8).

use std::collections::BTreeMap;

use ce_rs::locate::LocateOpts;
use ce_scale_types::Status;

use crate::resolve::Resolved;
use crate::version::VersionReq;

/// A declared dependency: a service name, the version it requires, the locate options to apply, and
/// whether its absence is fatal (design §3.1).
#[derive(Clone, Debug)]
pub struct Dep {
    pub name: String,
    pub req: VersionReq,
    pub opts: LocateOpts,
    /// If `true`, a missing dep at the deadline is simply absent from [`Bindings`]; if `false`, it
    /// fails the whole bind with a [`BindError`].
    pub optional: bool,
}

impl Dep {
    /// A required dependency with default locate options.
    pub fn required(name: impl Into<String>, req: VersionReq) -> Dep {
        Dep { name: name.into(), req, opts: LocateOpts::default(), optional: false }
    }

    /// An optional dependency with default locate options.
    pub fn optional(name: impl Into<String>, req: VersionReq) -> Dep {
        Dep { name: name.into(), req, opts: LocateOpts::default(), optional: true }
    }

    /// Set the locate options (tags, want, staleness) for this dep (builder style).
    pub fn with_opts(mut self, opts: LocateOpts) -> Dep {
        self.opts = opts;
        self
    }
}

/// The result of a successful [`Registry::bind`](crate::Registry::bind): each resolved dep keyed by
/// its declared name. Optional deps that did not resolve before the deadline are simply absent.
#[derive(Debug, Default)]
pub struct Bindings {
    map: BTreeMap<String, Resolved>,
}

impl Bindings {
    pub fn new() -> Bindings {
        Bindings { map: BTreeMap::new() }
    }

    /// The resolved instance bound to dependency `dep`, if it resolved.
    pub fn get(&self, dep: &str) -> Option<&Resolved> {
        self.map.get(dep)
    }

    /// Insert a resolved binding for `dep`.
    pub fn insert(&mut self, dep: impl Into<String>, resolved: Resolved) {
        self.map.insert(dep.into(), resolved);
    }

    /// The names of every dep that was bound.
    pub fn names(&self) -> impl Iterator<Item = &String> {
        self.map.keys()
    }

    /// How many deps were bound.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// True if nothing was bound.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

/// Why a [`Registry::bind`](crate::Registry::bind) failed: which dependency, and the [`Status`]
/// that made it unsatisfiable (design §3.1). `NotFound` = advertised by nobody; `Unavailable` =
/// advertised but none healthy; `FailedPrecondition` = healthy but no version match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindError {
    pub dep: String,
    pub status: Status,
}

impl std::fmt::Display for BindError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "dependency '{}' unsatisfiable: {}", self.dep, self.status)
    }
}

impl std::error::Error for BindError {}

/// The classification of one dep's current resolve attempt, fed to the bind decision core. This is
/// the abstraction the async loop produces per dep per retry; the decision logic is pure over it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DepOutcome {
    /// Resolved to an instance this round (carries no payload here; the loop keeps the `Resolved`).
    Resolved,
    /// Not resolvable yet, but the failure is *transient* (`NotFound`/`Unavailable`) — keep
    /// retrying until the deadline. The instance may simply not have started yet.
    Pending(Status),
    /// Permanently unsatisfiable this configuration — `FailedPrecondition` (no version match) or a
    /// caller fault. No amount of waiting helps; fail the bind immediately if the dep is required.
    Fatal(Status),
}

/// Map a resolve `Status` to its bind outcome class (design §4.5). A `NotFound`/`Unavailable` is
/// `Pending` (the instance may yet come up before the deadline); a `FailedPrecondition` (or any
/// other non-transient status) is `Fatal` (the declared version can never be satisfied by what is
/// advertised, so waiting is futile).
pub fn classify(status: Status) -> DepOutcome {
    match status {
        Status::Ok | Status::AlreadyApplied => DepOutcome::Resolved,
        Status::NotFound | Status::Unavailable | Status::DeadlineExceeded => {
            DepOutcome::Pending(status)
        }
        other => DepOutcome::Fatal(other),
    }
}

/// The terminal decision for one dependency once the deadline has elapsed (or it became fatal): is
/// the *overall* bind still ok, or has this dep failed it? Pure logic for the loop's exit:
///
/// * a required dep that is still `Pending` at the deadline ⇒ the bind fails with that dep's
///   transient status (e.g. `Unavailable`),
/// * a required dep that is `Fatal` ⇒ the bind fails immediately with the fatal status,
/// * an optional dep that never resolved ⇒ absent from `Bindings`, not a failure.
///
/// Returns `Some(BindError)` if this dep fails the bind, `None` if it is acceptable to omit.
pub fn terminal_decision(dep: &Dep, last: &DepOutcome) -> Option<BindError> {
    match last {
        DepOutcome::Resolved => None,
        DepOutcome::Pending(status) | DepOutcome::Fatal(status) => {
            if dep.optional {
                None
            } else {
                Some(BindError { dep: dep.name.clone(), status: *status })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(name: &str, optional: bool) -> Dep {
        Dep {
            name: name.into(),
            req: VersionReq::Any,
            opts: LocateOpts::default(),
            optional,
        }
    }

    #[test]
    fn classify_maps_statuses() {
        assert_eq!(classify(Status::Ok), DepOutcome::Resolved);
        assert_eq!(classify(Status::NotFound), DepOutcome::Pending(Status::NotFound));
        assert_eq!(classify(Status::Unavailable), DepOutcome::Pending(Status::Unavailable));
        assert_eq!(
            classify(Status::FailedPrecondition),
            DepOutcome::Fatal(Status::FailedPrecondition)
        );
        assert_eq!(classify(Status::Unauthorized), DepOutcome::Fatal(Status::Unauthorized));
    }

    #[test]
    fn required_pending_at_deadline_fails_bind() {
        let dep = req("db", false);
        let err = terminal_decision(&dep, &DepOutcome::Pending(Status::Unavailable));
        assert_eq!(err, Some(BindError { dep: "db".into(), status: Status::Unavailable }));
    }

    #[test]
    fn required_fatal_fails_bind_with_fatal_status() {
        let dep = req("db", false);
        let err = terminal_decision(&dep, &DepOutcome::Fatal(Status::FailedPrecondition));
        assert_eq!(err, Some(BindError { dep: "db".into(), status: Status::FailedPrecondition }));
    }

    #[test]
    fn optional_missing_is_not_a_failure() {
        let dep = req("cache", true);
        assert_eq!(terminal_decision(&dep, &DepOutcome::Pending(Status::NotFound)), None);
        assert_eq!(terminal_decision(&dep, &DepOutcome::Fatal(Status::FailedPrecondition)), None);
    }

    #[test]
    fn resolved_dep_never_fails_bind() {
        assert_eq!(terminal_decision(&req("db", false), &DepOutcome::Resolved), None);
        assert_eq!(terminal_decision(&req("db", true), &DepOutcome::Resolved), None);
    }

    /// Model the full bind decision over a dep set: all required resolved -> Ok; one required
    /// pending at deadline -> the first such BindError; optional missing omitted. This is the pure
    /// core of the async loop (design §8: "all-deps-satisfied returns Bindings; one unsatisfiable
    /// returns its BindError; optional missing dep is absent not fatal").
    fn decide(deps: &[(Dep, DepOutcome)]) -> Result<Vec<String>, BindError> {
        let mut bound = Vec::new();
        for (dep, outcome) in deps {
            match terminal_decision(dep, outcome) {
                Some(err) => return Err(err),
                None => {
                    if matches!(outcome, DepOutcome::Resolved) {
                        bound.push(dep.name.clone());
                    }
                }
            }
        }
        Ok(bound)
    }

    #[test]
    fn all_required_resolved_returns_bindings() {
        let deps = vec![
            (req("db", false), DepOutcome::Resolved),
            (req("storage", false), DepOutcome::Resolved),
        ];
        assert_eq!(decide(&deps).unwrap(), vec!["db".to_string(), "storage".to_string()]);
    }

    #[test]
    fn one_unsatisfiable_required_returns_its_error() {
        let deps = vec![
            (req("db", false), DepOutcome::Resolved),
            (req("storage", false), DepOutcome::Fatal(Status::FailedPrecondition)),
        ];
        let err = decide(&deps).unwrap_err();
        assert_eq!(err.dep, "storage");
        assert_eq!(err.status, Status::FailedPrecondition);
    }

    #[test]
    fn optional_missing_dep_is_absent_not_fatal() {
        let deps = vec![
            (req("db", false), DepOutcome::Resolved),
            (req("cache", true), DepOutcome::Pending(Status::NotFound)),
        ];
        // db is bound; cache is omitted; bind succeeds.
        assert_eq!(decide(&deps).unwrap(), vec!["db".to_string()]);
    }
}
