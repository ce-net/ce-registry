//! # ce-registry — CE scaling primitive 01: service registry, binding & health
//!
//! The "dependencies" mechanism of the CE scaling layer (`PLAN/scale/01-service-registry-binding.md`,
//! conforming to `PLAN/scale/00-architecture.md`). It answers the three questions plain DHT
//! `find_service` cannot:
//!
//! 1. **Is an instance healthy and ready right now?** — a per-instance [`Health`] contract carried
//!    in a multi-writer CRDT, not just "some node once advertised this string".
//! 2. **Which version?** — explicit [`SemVer`]/[`VersionReq`] matching, major on the DHT key,
//!    minor/patch matched client-side.
//! 3. **Are all my dependencies up before I start?** — [`Registry::bind`] resolves a declared dep
//!    set (`needs storage@^1, db@^2`) to live endpoints, failing fast and legibly.
//!
//! ## Plane: coord (no node changes)
//!
//! This is a pure **coord-plane** library (architecture §0/§0.1 row 01). It proposes **no new node
//! RPC**: a registration is a `ce-coord` [`Merged`](ce_coord::Merged) `propose` (a signed pub/sub
//! op) plus a periodic DHT re-advertise; a resolve is `find_service` + `atlas` + a read of the
//! health CRDT. Every register/resolve goes through the local node via `ce-rs`, which authenticates
//! the sender and verifies `Ctx.cap`. "Healthy", "version", and "dependency" are all app concepts;
//! the node stores only opaque DHT provider records and routes opaque messages.
//!
//! ## What is real here vs. what needs a live mesh
//!
//! The **deterministic cores are implemented and fully tested in-crate** (design §8):
//! [`VersionReq::matches`] (the binding core), the [`HealthBook`] CRDT convergence,
//! the resolve [`filter`](resolve::filter)/[`rank`](resolve::rank) pipeline over synthetic
//! fixtures, and the [`bind`](bind) decision logic. The `ce-coord` testkit
//! (`MockNode`) drives the health-book *convergence* end-to-end across in-process nodes (see
//! `tests/health_book.rs`). What genuinely needs a **live multi-node mesh** is full `resolve`/`bind`
//! (the `MockNode` broker implements pub/sub + blobs but not `find_service`/`atlas`/`history`), and
//! the end-to-end drain/crash/partition demos of §8's last tier. See `README.md`.
//!
//! ## Abilities (architecture §3)
//!
//! Reserves `svc:register`, `svc:resolve`, `svc:deregister`, resource-scoped by
//! `Tag("svc:<ns>/<name>")`. The local node verifies `Ctx.cap` before honoring a propose/find; this
//! crate, like all SDK code, never trusts a stranger — it is the node that enforces.
//!
//! ## Standards
//! edition 2024; `anyhow::Result` for fallible public fns; `tracing` (no `println!`); no `unsafe`;
//! no `unwrap`/`expect`/`panic!` in library paths; tests in `#[cfg(test)]`.

#![forbid(unsafe_code)]

pub mod bind;
pub mod health;
pub mod resolve;
pub mod version;

pub use bind::{Bindings, BindError, Dep};
pub use health::{
    DEFAULT_STALENESS_K, Health, HealthBook, HealthOp, InstanceId, Phase, stale_ms,
};
pub use resolve::{DEFAULT_LOAD_WEIGHT, Resolved};
pub use version::{SemVer, VersionReq};

// Re-export the shared contract types so app code writes `use ce_registry::Status` and gets the one
// canonical definition (architecture §2/§4).
pub use ce_scale_types::{Ctx, Partial, Status};

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Result, anyhow};
use ce_coord::{Coord, Merged};
use ce_rs::locate::LocateOpts;
use tokio::sync::Mutex;

use crate::bind::{DepOutcome, classify, terminal_decision};
use crate::resolve::{FilterOutcome, filter, rank};

/// The reserved abilities for the registry (architecture §3). Opaque `domain:verb` strings; the
/// `ce-cap` verifier never learns what they mean — it only checks the chain authorizes the exact
/// string on the resource `Tag("svc:<ns>/<name>")`.
pub const ABILITY_REGISTER: &str = "svc:register";
pub const ABILITY_RESOLVE: &str = "svc:resolve";
pub const ABILITY_DEREGISTER: &str = "svc:deregister";

/// The default health-refresh interval (design §4.3/§5: `refresh = 10s`).
pub const DEFAULT_REFRESH: Duration = Duration::from_secs(10);

/// Build the versioned DHT service-key string `"<ns>/<name>@<major>"` (design §4.1). Major-only:
/// a major bump is a breaking change and a distinct service. Reuses the node's `service_key`
/// hashing unchanged (this is just the human-readable string the node hashes).
pub fn service_key_str(ns: &str, name: &str, major: u32) -> String {
    format!("{ns}/{name}@{major}")
}

/// The `ce-coord` replica/book name for `(ns, name@major)` (design conformance §6: the health CRDT
/// replica key is `(ns, "svc/<name>@<major>")`). `Merged`/`Replicated` derive their pub/sub topics
/// from this name, so two namespaces or majors never share a book.
pub fn book_name(ns: &str, name: &str, major: u32) -> String {
    format!("{ns}::svc/{name}@{major}")
}

/// Attach the registry to the local node's coordination layer (design §3.1 `Registry::open`).
///
/// Holds the [`Coord`] (the node's coordination layer over `ce-rs`) and lazily opens one
/// [`Merged<HealthBook>`] per `(ns, name@major)` it touches. Cheap to clone-share via the inner
/// `Arc`.
#[derive(Clone)]
pub struct Registry {
    inner: Arc<RegistryInner>,
}

struct RegistryInner {
    coord: Coord,
    /// One health book per `(ns, name@major)`, opened lazily and cached.
    books: Mutex<std::collections::HashMap<String, Arc<Merged<HealthBook>>>>,
    /// Tunable: how much self-reported load moves ranking (design §4.4, `rank`).
    load_weight: f64,
    /// Tunable: staleness multiplier `k` (design §4.4/§5).
    staleness_k: u32,
}

impl Registry {
    /// Attach to the local node's coordination layer.
    pub async fn open(coord: Coord) -> Result<Registry> {
        Ok(Registry {
            inner: Arc::new(RegistryInner {
                coord,
                books: Mutex::new(std::collections::HashMap::new()),
                load_weight: DEFAULT_LOAD_WEIGHT,
                staleness_k: DEFAULT_STALENESS_K,
            }),
        })
    }

    /// This node's NodeId hex (it is the writer of its own health rows).
    pub fn node_id(&self) -> &str {
        self.inner.coord.node_id()
    }

    /// Open (or fetch the cached) health book for `(ns, name@major)`. Following the providers the
    /// DHT reports as writers happens in [`resolve`](Self::resolve) via `add_writer`.
    async fn book(&self, ns: &str, name: &str, major: u32) -> Result<Arc<Merged<HealthBook>>> {
        let key = book_name(ns, name, major);
        {
            let books = self.inner.books.lock().await;
            if let Some(b) = books.get(&key) {
                return Ok(b.clone());
            }
        }
        // Open with no peers; resolve() learns writers from find_service and add_writer()s them.
        let self_id = self.inner.coord.node_id().to_string();
        let merged =
            Merged::<HealthBook>::open(&self.inner.coord, &key, &self_id, &[]).await?;
        let arc = Arc::new(merged);
        let mut books = self.inner.books.lock().await;
        Ok(books.entry(key).or_insert(arc).clone())
    }

    // ---- producer side --------------------------------------------------------------------------

    /// Register THIS node as an instance of `name` at `version` in namespace `ns`, publishing
    /// `initial` health (design §3.1, §4.3).
    ///
    /// Spawns two background tasks until the returned [`Registration`] is dropped:
    /// (a) periodic DHT re-advertise of `service_key_str(ns,name,major)` (DHT provider records
    /// expire; re-advertising is the reachability heartbeat), and (b) a health refresh every
    /// `refresh` that `propose`s a fresh [`Health`] with `epoch+1` and the *current* wall clock
    /// into the book. Dropping the handle publishes `Phase::Draining` once and stops both tasks.
    ///
    /// Authorization: the local node verifies the caller holds `svc:register` on
    /// `Tag("svc:<ns>/<name>")` before honoring the advertise/propose (design §6); this method does
    /// not duplicate that check — it relies on the node as the enforcement point.
    pub async fn register(
        &self,
        ns: &str,
        name: &str,
        version: SemVer,
        initial: Health,
        refresh: Duration,
    ) -> Result<Registration> {
        let major = version.major;
        let book = self.book(ns, name, major).await?;
        let self_id = self.inner.coord.node_id().to_string();
        let service = service_key_str(ns, name, major);

        // Publish the initial record (its epoch is whatever the caller set; subsequent refreshes
        // strictly increase from there).
        let epoch = Arc::new(AtomicU64::new(initial.epoch));
        let phase = Arc::new(Mutex::new(initial.phase));
        let mut first = initial.clone();
        first.version = version;
        first.reported_at_ms = now_ms();
        book.propose(HealthOp { writer: self_id.clone(), health: first }).await?;

        // Advertise on the DHT now and periodically.
        if let Err(e) = self.inner.coord.client().advertise_service(&service).await {
            tracing::warn!(%service, error = %e, "register: initial advertise failed; will retry");
        }

        let stop = Arc::new(AtomicBool::new(false));
        let load = Arc::new(Mutex::new(initial.load));
        let meta = Arc::new(Mutex::new(initial.meta.clone()));
        let fault_domain = initial.fault_domain.clone();

        // Background refresh + re-advertise loop.
        let ce = self.inner.coord.client().clone();
        let book_bg = book.clone();
        let self_id_bg = self_id.clone();
        let stop_bg = stop.clone();
        let epoch_bg = epoch.clone();
        let phase_bg = phase.clone();
        let load_bg = load.clone();
        let meta_bg = meta.clone();
        let service_bg = service.clone();
        let fd_bg = fault_domain.clone();
        let handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(refresh).await;
                if stop_bg.load(Ordering::SeqCst) {
                    return;
                }
                // Re-advertise (reachability heartbeat).
                if let Err(e) = ce.advertise_service(&service_bg).await {
                    tracing::warn!(service = %service_bg, error = %e, "register: re-advertise failed");
                }
                // Publish a fresh health record with a strictly-increasing epoch.
                let next = epoch_bg.fetch_add(1, Ordering::SeqCst) + 1;
                let h = Health {
                    version,
                    phase: *phase_bg.lock().await,
                    load: *load_bg.lock().await,
                    epoch: next,
                    reported_at_ms: now_ms(),
                    fault_domain: fd_bg.clone(),
                    meta: meta_bg.lock().await.clone(),
                };
                if let Err(e) =
                    book_bg.propose(HealthOp { writer: self_id_bg.clone(), health: h }).await
                {
                    tracing::warn!(error = %e, "register: health refresh propose failed");
                }
            }
        });

        Ok(Registration {
            book,
            self_id,
            version,
            fault_domain,
            epoch,
            phase,
            load,
            meta,
            stop,
            handle: Some(handle),
        })
    }

    // ---- consumer side --------------------------------------------------------------------------

    /// Resolve ONE healthy instance of `name` satisfying `req`, ranked best-first (design §3.1,
    /// §4.4). Filters by the health contract (Ready + fresh + version-matched) THEN applies locate's
    /// atlas/trust/recency ranking intersected with self-reported load.
    ///
    /// Errors map to the contract's `Status` (architecture §4): `NotFound` (advertised by nobody),
    /// `Unavailable` (advertised but none healthy), `FailedPrecondition` (healthy but none version-
    /// matched).
    pub async fn resolve(
        &self,
        ns: &str,
        name: &str,
        req: &VersionReq,
        opts: &LocateOpts,
    ) -> Result<Resolved, Status> {
        let mut ranked = self.resolve_ranked(ns, name, req, opts).await?;
        if ranked.is_empty() {
            // resolve_ranked only returns Ok with a non-empty vec; empty is unreachable, but stay safe.
            return Err(Status::NotFound);
        }
        Ok(ranked.remove(0))
    }

    /// Resolve up to `opts.want` healthy instances (redundancy / sharded fan-out), as a
    /// [`Partial<Resolved>`] so the caller sees which instances are live (design §3.1). The `ok`
    /// list holds the resolved instances keyed by their NodeId bytes; `failed` is empty here (a
    /// resolve either yields live instances or an error) but the `Partial` shape is what 02/05
    /// consume.
    pub async fn resolve_many(
        &self,
        ns: &str,
        name: &str,
        req: &VersionReq,
        opts: &LocateOpts,
    ) -> Partial<Resolved> {
        let mut partial = Partial::new();
        match self.resolve_ranked(ns, name, req, opts).await {
            Ok(ranked) => {
                let want = opts.want.max(1);
                for r in ranked.into_iter().take(want) {
                    let id = node_id_bytes(r.node_id());
                    partial.push_ok(id, r);
                }
            }
            Err(status) => {
                // No instances; record the status with a zero node id so the caller sees *why*
                // without a successful resolution being implied.
                partial.push_failed([0u8; 32], status);
            }
        }
        partial
    }

    /// The shared discover → join-health → filter → rank pipeline behind `resolve`/`resolve_many`.
    /// Returns the ranked live, version-matched instances, or a `Status` describing why none.
    async fn resolve_ranked(
        &self,
        ns: &str,
        name: &str,
        req: &VersionReq,
        opts: &LocateOpts,
    ) -> Result<Vec<Resolved>, Status> {
        let ce = self.inner.coord.client();

        // Discover providers across every compatible major. We query the requirement's pinned
        // major(s); for an open Gte/Any we probe a small ascending range from the floor until a
        // major yields nothing (a cheap bounded scan — the DHT has no "list majors" verb).
        let majors = self.candidate_majors(ns, name, req).await;
        let mut all_ids: Vec<String> = Vec::new();
        let mut book_for_major: std::collections::HashMap<u32, Arc<Merged<HealthBook>>> =
            std::collections::HashMap::new();
        for m in majors {
            let service = service_key_str(ns, name, m);
            let ids = match ce.find_service(&service).await {
                Ok(ids) => ids,
                Err(_) => continue,
            };
            if ids.is_empty() {
                continue;
            }
            // Open/cached book for this major; follow every discovered provider as a writer.
            let book = match self.book(ns, name, m).await {
                Ok(b) => b,
                Err(_) => continue,
            };
            for id in &ids {
                let _ = book.add_writer(id).await;
            }
            book.pull();
            book_for_major.insert(m, book);
            all_ids.extend(ids);
        }

        if all_ids.is_empty() {
            return Err(Status::NotFound);
        }

        // Rank the discovered ids via locate (atlas/trust/recency). We ask locate for enough
        // candidates to cover redundancy + failover; it filters atlas-staleness and required tags.
        let mut locate_opts = opts.clone();
        locate_opts.want = opts.want.max(8);
        // locate works on the major-keyed service string; merge results across majors.
        let mut instances: Vec<ce_rs::locate::Instance> = Vec::new();
        for m in book_for_major.keys() {
            let service = service_key_str(ns, name, *m);
            if let Ok(found) = ce_rs::locate::locate(ce, &service, &locate_opts).await {
                instances.extend(found);
            }
        }
        if instances.is_empty() {
            // Advertised on the DHT but none in the atlas / matching tags -> treat as Unavailable.
            return Err(Status::Unavailable);
        }

        // Merge every major's book into one lookup (a node only appears under its own major).
        let now = now_ms();
        let window = stale_ms(refresh_ms_default(), self.inner.staleness_k);
        let mut merged_book = HealthBook::default();
        for book in book_for_major.values() {
            book.read(|b| {
                for (id, h) in b.entries() {
                    use ce_coord::MergeMachine;
                    merged_book.apply(HealthOp { writer: id.clone(), health: h.clone() });
                }
            });
        }

        let outcome: FilterOutcome = filter(&instances, &merged_book, req, now, window);
        if outcome.kept.is_empty() {
            return Err(status_for_empty(&outcome));
        }
        Ok(rank(outcome.kept, self.inner.load_weight))
    }

    /// The set of majors whose DHT keys to query for `req` (design §4.1/§4.4). A pinned
    /// `Caret`/`Tilde`/`Exact` yields its single major; an open `Gte`/`Any` is probed by an
    /// ascending bounded scan from the floor, stopping at the first major with no providers (so we
    /// never scan unboundedly).
    async fn candidate_majors(&self, ns: &str, name: &str, req: &VersionReq) -> Vec<u32> {
        match req {
            VersionReq::Caret(a, _, _) | VersionReq::Tilde(a, _, _) => vec![*a],
            VersionReq::Exact(v) => vec![v.major],
            VersionReq::Gte(_) | VersionReq::Any => {
                let ce = self.inner.coord.client();
                let floor = req.major_floor();
                let mut seen = Vec::new();
                let mut misses = 0u32;
                // Probe floor..floor+32, tolerating up to 2 consecutive empty majors (sparse gaps).
                for m in floor..floor.saturating_add(32) {
                    let service = service_key_str(ns, name, m);
                    match ce.find_service(&service).await {
                        Ok(ids) if !ids.is_empty() => {
                            seen.push(m);
                            misses = 0;
                        }
                        _ => {
                            misses += 1;
                            if misses > 2 && !seen.is_empty() {
                                break;
                            }
                        }
                    }
                }
                if seen.is_empty() { vec![floor] } else { seen }
            }
        }
    }

    /// Resolve a whole dependency set at startup (design §3.1, §4.5) — the literal "needs storage@^1,
    /// db@^2" mechanism. Blocks up to `deadline` until EVERY required dep has at least one healthy
    /// instance, returning the bound endpoints keyed by dep name; returns the first unsatisfiable
    /// required dep as a [`BindError`]. Optional deps that miss the deadline are simply absent.
    ///
    /// A transient miss (`NotFound`/`Unavailable`) retries until the deadline (the instance may yet
    /// start); a fatal status (`FailedPrecondition` = the declared version can never be satisfied by
    /// what is advertised) fails a required dep immediately without waiting.
    pub async fn bind(
        &self,
        ns: &str,
        deps: &[Dep],
        deadline: Duration,
    ) -> Result<Bindings, BindError> {
        let start = std::time::Instant::now();
        let mut bindings = Bindings::new();
        let mut pending: Vec<&Dep> = deps.iter().collect();

        loop {
            let mut still_pending: Vec<&Dep> = Vec::new();
            for dep in pending {
                match self.resolve(ns, &dep.name, &dep.req, &dep.opts).await {
                    Ok(resolved) => {
                        bindings.insert(dep.name.clone(), resolved);
                    }
                    Err(status) => match classify(status) {
                        DepOutcome::Resolved => {} // unreachable (Err arm)
                        DepOutcome::Fatal(s) => {
                            if let Some(err) = terminal_decision(dep, &DepOutcome::Fatal(s)) {
                                return Err(err);
                            }
                            // optional + fatal -> omit
                        }
                        DepOutcome::Pending(s) => {
                            // Will retry until the deadline; remember why for the terminal decision.
                            if start.elapsed() >= deadline {
                                if let Some(err) =
                                    terminal_decision(dep, &DepOutcome::Pending(s))
                                {
                                    return Err(err);
                                }
                                // optional + pending past deadline -> omit
                            } else {
                                still_pending.push(dep);
                            }
                        }
                    },
                }
            }
            if still_pending.is_empty() {
                return Ok(bindings);
            }
            if start.elapsed() >= deadline {
                // Anything still pending at the deadline: decide each (required -> error, opt -> omit).
                for dep in still_pending {
                    if let Some(err) =
                        terminal_decision(dep, &DepOutcome::Pending(Status::Unavailable))
                    {
                        return Err(err);
                    }
                }
                return Ok(bindings);
            }
            pending = still_pending;
            // Wait a short interval before re-resolving (a health-book change would arrive via the
            // pump in the background; this bounded poll keeps the loop simple and deadline-honest).
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
}

/// Choose the contract `Status` for an empty `kept` set from the filter tally (design §4.4): a
/// version-mismatch among live instances is `FailedPrecondition`; any advertised provider with no
/// live health is `Unavailable`; nothing advertised is `NotFound`.
fn status_for_empty(outcome: &FilterOutcome) -> Status {
    if outcome.version_mismatch > 0 {
        Status::FailedPrecondition
    } else if outcome.advertised() > 0 {
        Status::Unavailable
    } else {
        Status::NotFound
    }
}

/// A live registration handle (design §3.1 `Registration`). RAII: dropping it publishes
/// `Phase::Draining` and stops the background refresh/re-advertise tasks. While held, the instance's
/// phase/load/meta can be mutated and the next refresh publishes them.
pub struct Registration {
    book: Arc<Merged<HealthBook>>,
    self_id: String,
    version: SemVer,
    fault_domain: Option<String>,
    epoch: Arc<AtomicU64>,
    phase: Arc<Mutex<Phase>>,
    load: Arc<Mutex<f32>>,
    meta: Arc<Mutex<std::collections::BTreeMap<String, String>>>,
    stop: Arc<AtomicBool>,
    handle: Option<tokio::task::JoinHandle<()>>,
}

impl Registration {
    /// Transition the instance's phase (e.g. `Starting` → `Ready` after warming caches) and publish
    /// it immediately so resolvers see the change without waiting for the next refresh tick.
    pub async fn set_phase(&self, phase: Phase) -> Result<()> {
        *self.phase.lock().await = phase;
        self.publish_now().await
    }

    /// Update the self-reported load (07-telemetry SLI) and publish it on the next refresh.
    pub async fn set_load(&self, load: f32) {
        *self.load.lock().await = load.clamp(0.0, 1.0);
    }

    /// Replace the app `meta` map (endpoint topic, shard range, …).
    pub async fn set_meta(&self, meta: std::collections::BTreeMap<String, String>) {
        *self.meta.lock().await = meta;
    }

    /// Publish a fresh health record right now with a strictly-increasing epoch (used by
    /// `set_phase` and on drain).
    async fn publish_now(&self) -> Result<()> {
        let next = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;
        let h = Health {
            version: self.version,
            phase: *self.phase.lock().await,
            load: *self.load.lock().await,
            epoch: next,
            reported_at_ms: now_ms(),
            fault_domain: self.fault_domain.clone(),
            meta: self.meta.lock().await.clone(),
        };
        self.book
            .propose(HealthOp { writer: self.self_id.clone(), health: h })
            .await
            .map(|_| ())
    }
}

impl Drop for Registration {
    /// Deregister (design §4.3 step 4): publish `Phase::Draining` once (best-effort) and stop the
    /// background tasks. A hard crash skips this; readers age the instance out by staleness.
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            h.abort();
        }
        // Best-effort drain publish. We can't `.await` in Drop, so spawn a detached task that
        // proposes a final Draining record with a higher epoch.
        let book = self.book.clone();
        let self_id = self.self_id.clone();
        let version = self.version;
        let fault_domain = self.fault_domain.clone();
        let epoch = self.epoch.clone();
        let meta = self.meta.clone();
        let next = epoch.fetch_add(1, Ordering::SeqCst) + 1;
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let h = Health {
                    version,
                    phase: Phase::Draining,
                    load: 1.0,
                    epoch: next,
                    reported_at_ms: now_ms(),
                    fault_domain,
                    meta: meta.lock().await.clone(),
                };
                let _ = book.propose(HealthOp { writer: self_id, health: h }).await;
            });
        }
    }
}

/// Current wall-clock time in unix-ms (the `reported_at_ms` source; mirrors `Ctx.hlc.0`).
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The default refresh interval in ms, used to derive the staleness window when a resolver has no
/// per-service refresh known (it filters on the contract's default `k * refresh`).
fn refresh_ms_default() -> u64 {
    DEFAULT_REFRESH.as_millis() as u64
}

/// Convert a NodeId hex string to the 32-byte form `Partial<T>` keys on. A malformed/short id maps
/// to a zero-padded best-effort array (never panics).
fn node_id_bytes(hex_id: &str) -> [u8; 32] {
    let mut out = [0u8; 32];
    let mut i = 0;
    let bytes = hex_id.as_bytes();
    while i < 32 && (i * 2 + 1) < bytes.len() {
        let hi = hex_val(bytes[i * 2]);
        let lo = hex_val(bytes[i * 2 + 1]);
        if let (Some(hi), Some(lo)) = (hi, lo) {
            out[i] = (hi << 4) | lo;
        }
        i += 1;
    }
    out
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_key_and_book_name_are_versioned_and_namespaced() {
        assert_eq!(service_key_str("game/prod", "match-host", 2), "game/prod/match-host@2");
        assert_eq!(book_name("game/prod", "match-host", 2), "game/prod::svc/match-host@2");
        // distinct majors and namespaces never collide
        assert_ne!(book_name("a", "x", 1), book_name("a", "x", 2));
        assert_ne!(book_name("a", "x", 1), book_name("b", "x", 1));
    }

    #[test]
    fn status_for_empty_decision_table() {
        // version mismatch among live -> FailedPrecondition
        let mut o = FilterOutcome::default();
        o.version_mismatch = 2;
        assert_eq!(status_for_empty(&o), Status::FailedPrecondition);

        // advertised but none live -> Unavailable
        let mut o = FilterOutcome::default();
        o.not_ready = 1;
        o.stale = 1;
        assert_eq!(status_for_empty(&o), Status::Unavailable);

        // nothing advertised -> NotFound
        let o = FilterOutcome::default();
        assert_eq!(status_for_empty(&o), Status::NotFound);

        // version mismatch wins over plain unavailability
        let mut o = FilterOutcome::default();
        o.version_mismatch = 1;
        o.not_ready = 5;
        assert_eq!(status_for_empty(&o), Status::FailedPrecondition);
    }

    #[test]
    fn node_id_bytes_roundtrips_and_tolerates_garbage() {
        let id = "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff";
        let b = node_id_bytes(id);
        assert_eq!(b[0], 0x00);
        assert_eq!(b[1], 0x11);
        assert_eq!(b[31], 0xff);
        // short / malformed never panics
        assert_eq!(node_id_bytes("zz"), [0u8; 32]);
        assert_eq!(node_id_bytes(""), [0u8; 32]);
    }

    #[test]
    fn ability_constants_follow_convention() {
        for a in [ABILITY_REGISTER, ABILITY_RESOLVE, ABILITY_DEREGISTER] {
            assert!(a.starts_with("svc:"));
            assert_eq!(a.matches(':').count(), 1, "single colon domain:verb");
        }
    }
}
