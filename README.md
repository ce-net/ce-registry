# ce-registry — CE scaling primitive 01: service registry, binding & health

The **"dependencies" mechanism** of the CE scaling layer: discover a *healthy, versioned* service
instance, declare and resolve app dependencies, and carry the per-instance health/readiness
contract. The contract in one line: a service registry plus dependency binding over a shared
HealthBook — SemVer version constraints and health-aware resolve, as a coord-plane library with
no new node RPCs.

It builds on:

- **`ce-scale-types`** — the `Ctx` envelope, `Status`, and `Partial<T>` (re-exported here).
- **`ce-rs`** (`locate` feature) — DHT discovery (`advertise_service`/`find_service`), the `atlas`,
  and the `locate.rs` trust/capacity/recency ranking this registry reuses wholesale.
- **`ce-coord`** — the multi-writer `Merged` CRDT engine the health book is built on, plus `Coord`.

It is a pure **coord-plane** library: **no new node RPCs**. A registration is a `Merged::propose`
(a signed pub/sub op) plus a periodic DHT re-advertise; a resolve is `find_service` + `atlas` + a
read of the health CRDT. Every call goes through the local node, which authenticates the sender and
verifies `Ctx.cap`.

## What plain `find_service` could not answer (and this does)

| Axis | `find_service` | `ce-registry` |
|---|---|---|
| **Healthy & ready now?** | a provider record merely expires | a per-instance `Health{phase, load, epoch, reported_at_ms}` contract; only `Ready` + fresh instances resolve |
| **Which version?** | no version axis | `SemVer` + `VersionReq` (`^`/`~`/`>=`/`=`/`*`); major on the DHT key, minor/patch matched client-side |
| **All my deps up before I start?** | n/a | `bind(["storage@^1","db@^2"])` resolves the whole set or fails fast with which dep and why |

## Public API (design §3)

- `version` — `SemVer`, `VersionReq`, and the pure `matches()` binding core (+ `major_floor`,
  `compatible_major`, `query_majors` for choosing which DHT keys to query).
- `health` — the `Health`/`Phase` contract and the `HealthBook` `MergeMachine`: a
  `NodeId -> Health` map with **per-writer last-write-wins by epoch**, so concurrent
  registrations/heartbeats from N instances converge with no coordinator.
- `resolve` — `Resolved`, and the deterministic **filter-then-rank** pipeline: filter by the health
  contract (Ready + fresh + version-matched), then rank by locate's atlas/trust/recency signals
  intersected with self-reported load. `FilterOutcome` tallies *why* candidates dropped so the
  resolver can return `NotFound` vs `Unavailable` vs `FailedPrecondition`.
- `bind` — `Dep`, `Bindings`, `BindError`, and the dependency-resolution decision core (classify a
  resolve `Status` as resolved / transient-pending / fatal; the terminal decision for required vs
  optional deps).
- `Registry` (lib root) — `open`, `register` (RAII `Registration` that drains on drop),
  `resolve`, `resolve_many` (→ `Partial<Resolved>`), and `bind`. Reserves the abilities
  `svc:register` / `svc:resolve` / `svc:deregister`, resource-scoped by `Tag("svc:<ns>/<name>")`.

### Data structures (design §4)

- Versioned DHT key: `service_key_str(ns, name, major)` = `"<ns>/<name>@<major>"` (major-only on the
  DHT key — a major bump is a breaking change → a distinct service).
- Health book replica key: `book_name(ns, name, major)` = `"<ns>::svc/<name>@<major>"` (one `Merged`
  per `(ns, name@major)`).

## Semantics & failure modes (design §5)

- **Eventually-consistent CRDT** health book: at-least-once, gap- and reorder-tolerant. A stale view
  degrades (one failed-then-retried call) — it never lies.
- **Staleness = missed epochs**: an instance is dead-to-readers when `now - reported_at_ms > k *
  refresh` (default `k = 3`, `refresh = 10s` → ≤ 30s crash detection), independent of DHT TTL.
- **Partition fail-safe**: a partitioned instance goes stale and is excluded (treated as
  unavailable, never falsely Ready); on heal its epochs resume and it re-enters. No election, so no
  split-brain.
- Self-reported `phase`/`load` are **hints, not proofs**; the trust gradient (locate's on-chain
  delivered-work ranking) and redundancy (`resolve_many` + compare) are the defense against a
  malicious instance claiming `Ready`.

## Security (design §6)

- Abilities are opaque `svc:register` / `svc:resolve` / `svc:deregister` strings, scoped by
  `Tag("svc:<ns>/<name>")`. The **local node** is the enforcement point (verifies `Ctx.cap`); this
  SDK never trusts a stranger.
- Every health op is a signed `Merged` op; a reader applies only ops from the writer it follows, so
  no instance can publish health *as another* (`key = (writer, epoch)`, writer = node-verified
  sender). The book admits only writers in the DHT provider set; per-writer LWW-by-epoch caps each
  writer to one effective row regardless of flood.

## What is tested in-crate vs. what needs a live mesh

**Implemented for real and fully tested here (design §8 deterministic cores):**

- `VersionReq::matches` — caret/tilde/gte/exact/any, leading-zero caret rules, DHT-key major
  selection; unit + property tests (`src/version.rs`).
- `HealthBook::apply` — older-epoch-ignored, higher-epoch-wins, independent writers, idempotent
  under reorder/duplication; convergence property tests mirroring `ce-coord`'s merge tests
  (`src/health.rs`).
- The resolve `filter`/`rank` pipeline over synthetic `(instances, book, now)` fixtures:
  Starting/Draining/Unhealthy excluded, stale excluded, version-mismatch excluded, the
  `NotFound`/`Unavailable`/`FailedPrecondition` distinction, ranking by locate score then load
  (`src/resolve.rs`).
- `bind` decision logic — all-required-resolved → `Bindings`; one unsatisfiable required →
  `BindError`; optional missing → absent not fatal; transient vs fatal classification
  (`src/bind.rs`).
- The `status_for_empty` decision table and the versioned key/book-name namespacing (`src/lib.rs`).

**Driven end-to-end through the `ce-coord` testkit (in-process multi-node, `tests/health_book.rs`):**
the **real** `Merged<HealthBook>` convergence — two instances register and a reader converges to
both; a `Draining` record overrides an earlier `Ready`; a concurrent registration storm from K
instances converges to the LWW-of-latest-epochs book. The mock broker implements pub/sub + blobs,
which is exactly the surface the health book rides.

**Needs a live multi-node mesh (documented, not faked):** full `Registry::resolve`/`resolve_many`/
`bind` — these require the node's DHT (`find_service`/`advertise_service`) and `atlas`, which the
in-process broker does not implement. The end-to-end §8 demo (start `db@2` + `storage@1`, `bind`
from a third node, kill the db and watch re-resolution, partition the relay and watch fail-safe
exclusion then re-entry) runs over real relayed NAT'd peers (laptop ↔ desktop ↔ relay).

## Deferred (honest)

Per the design's "minimal first slice" (§10), this ships slices 1–4: `SemVer`/`VersionReq`,
`HealthBook`, `Registry::{open, register, resolve, resolve_many}`, and static-deadline `bind`.
**Deferred to slice 2** (documented, not stubbed):

- `bind_live` — a watch-driven self-healing binding (`watch::Receiver<Result<Resolved,Status>>` per
  dep, re-resolving on every `Merged::watch` tick). `bind` covers the static startup case; the live
  binding is built on the same resolve core and is the natural next increment.
- `resolve_many` fault-domain spread — `resolve_many` returns the ranked live set as a
  `Partial<Resolved>`; lifting `locate::spread` to cross fault domains for redundancy is the slice-2
  refinement.
- Paid/private registries (metered `svc:resolve` via a payment channel) and the TS SDK mirror.
