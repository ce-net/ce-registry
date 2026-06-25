//! Integration test (design §8, in-process multi-node testkit tier): drive the **real**
//! `Merged<HealthBook>` machinery across several in-process nodes sharing one mock CE broker, and
//! assert that every reader converges to the same healthy set, that concurrent registrations from K
//! instances converge to the LWW-of-latest-epochs book, and that a `Draining` record overrides an
//! earlier `Ready`.
//!
//! This uses `ce_coord::testkit::MockNode` — a pub/sub broker + blob store, exactly the surface the
//! health book rides. It does NOT implement `find_service`/`atlas`/`history`, so the full
//! `Registry::resolve`/`bind` (which need DHT discovery + the atlas) cannot be exercised here; those
//! are covered by the deterministic filter/rank/bind cores in the crate and need a live mesh for an
//! end-to-end demo (see README). What this proves is the multi-writer convergence the resolver
//! depends on.

use std::collections::BTreeMap;
use std::time::Duration;

use ce_coord::testkit::{MockNode, within};
use ce_coord::{Coord, MergeMachine, Merged};
use ce_registry::{Health, HealthBook, HealthOp, Phase, SemVer, book_name};

const CONV: Duration = Duration::from_secs(10);

fn health(epoch: u64, phase: Phase, ver: SemVer) -> Health {
    Health {
        version: ver,
        phase,
        load: 0.1,
        epoch,
        reported_at_ms: epoch * 1000,
        fault_domain: Some("region:eu".into()),
        meta: BTreeMap::new(),
    }
}

/// Open a Merged<HealthBook> for a writer `coord` and a set of reader `coord`s, all following each
/// other, for the same `(ns,name@major)` book.
async fn open_book(coord: &Coord, book: &str, peers: &[String]) -> anyhow::Result<Merged<HealthBook>> {
    let self_id = coord.node_id().to_string();
    Merged::<HealthBook>::open(coord, book, &self_id, peers).await
}

#[tokio::test]
async fn two_instances_register_and_a_reader_converges() -> anyhow::Result<()> {
    let node = MockNode::start();
    let a = Coord::with_client(node.client()).await?;
    let b = Coord::with_client(node.client()).await?;
    let reader = Coord::with_client(node.client()).await?;
    let (a_id, b_id) = (a.node_id().to_string(), b.node_id().to_string());

    let book = book_name("game/prod", "match-host", 2);
    let v = SemVer::new(2, 3, 0);

    // Each instance writes its OWN row.
    let a_book = open_book(&a, &book, &[]).await?;
    let b_book = open_book(&b, &book, &[]).await?;
    let r_book = open_book(&reader, &book, &[a_id.clone(), b_id.clone()]).await?;

    a_book.propose(HealthOp { writer: a_id.clone(), health: health(1, Phase::Ready, v) }).await?;
    b_book.propose(HealthOp { writer: b_id.clone(), health: health(1, Phase::Ready, v) }).await?;

    // The reader converges to both healthy rows.
    let converged = within(CONV, || {
        r_book.pull();
        r_book.read(|book| book.len() == 2)
    })
    .await;
    assert!(converged, "reader did not converge to both instances' health rows");

    r_book.read(|book| {
        assert_eq!(book.get(&a_id).map(|h| h.phase), Some(Phase::Ready));
        assert_eq!(book.get(&b_id).map(|h| h.phase), Some(Phase::Ready));
        let live: Vec<_> = book.live_instances(2_000, 1_000_000).into_iter().map(|(id, _)| id.clone()).collect();
        assert_eq!(live.len(), 2, "both instances live");
    });
    Ok(())
}

#[tokio::test]
async fn draining_record_overrides_earlier_ready() -> anyhow::Result<()> {
    let node = MockNode::start();
    let a = Coord::with_client(node.client()).await?;
    let reader = Coord::with_client(node.client()).await?;
    let a_id = a.node_id().to_string();

    let book = book_name("game/prod", "match-host", 2);
    let v = SemVer::new(2, 0, 0);
    let a_book = open_book(&a, &book, &[]).await?;
    let r_book = open_book(&reader, &book, &[a_id.clone()]).await?;

    a_book.propose(HealthOp { writer: a_id.clone(), health: health(1, Phase::Ready, v) }).await?;
    let ready = within(CONV, || {
        r_book.pull();
        r_book.read(|b| b.get(&a_id).map(|h| h.phase) == Some(Phase::Ready))
    })
    .await;
    assert!(ready, "reader never saw the initial Ready");

    // The instance drains (higher epoch).
    a_book.propose(HealthOp { writer: a_id.clone(), health: health(2, Phase::Draining, v) }).await?;
    let drained = within(CONV, || {
        r_book.pull();
        r_book.read(|b| b.get(&a_id).map(|h| h.phase) == Some(Phase::Draining))
    })
    .await;
    assert!(drained, "reader did not see the Draining override");

    // A Draining instance is excluded from the live set.
    r_book.read(|b| {
        assert!(b.live_instances(2_000, 1_000_000).is_empty(), "drained instance must not be live");
    });
    Ok(())
}

#[tokio::test]
async fn concurrent_registration_storm_converges_to_latest_epochs() -> anyhow::Result<()> {
    // K instances each refresh their row several times concurrently; a reader must converge to the
    // LWW-of-latest-epochs book (design §8 integration tier: "concurrent register/deregister storms
    // converge; final book == LWW of latest epochs").
    let node = MockNode::start();
    let v = SemVer::new(1, 0, 0);
    let book = book_name("ce-db", "acme", 1);

    const K: usize = 4;
    const REFRESHES: u64 = 5;

    let mut coords = Vec::new();
    let mut ids = Vec::new();
    for _ in 0..K {
        let c = Coord::with_client(node.client()).await?;
        ids.push(c.node_id().to_string());
        coords.push(c);
    }

    // Each instance opens its own book and proposes REFRESHES increasing-epoch records.
    let mut writers = Vec::new();
    for c in &coords {
        writers.push(open_book(c, &book, &[]).await?);
    }
    let reader_coord = Coord::with_client(node.client()).await?;
    let r_book = open_book(&reader_coord, &book, &ids).await?;

    // Fire all writers concurrently.
    let mut tasks = Vec::new();
    for (i, w) in writers.into_iter().enumerate() {
        let id = ids[i].clone();
        tasks.push(tokio::spawn(async move {
            for e in 1..=REFRESHES {
                let phase = if e == REFRESHES { Phase::Ready } else { Phase::Starting };
                let _ = w.propose(HealthOp { writer: id.clone(), health: health(e, phase, v) }).await;
            }
        }));
    }
    for t in tasks {
        let _ = t.await;
    }

    // The reader converges to K rows, each at the latest epoch (REFRESHES) and Phase::Ready.
    let converged = within(CONV, || {
        r_book.pull();
        r_book.read(|b| {
            b.len() == K && b.entries().all(|(_, h)| h.epoch == REFRESHES && h.phase == Phase::Ready)
        })
    })
    .await;
    assert!(converged, "reader did not converge to K latest-epoch Ready rows");

    r_book.read(|b| {
        for id in &ids {
            let h = b.get(id).expect("each instance has a row");
            assert_eq!(h.epoch, REFRESHES, "row is at the latest epoch (LWW)");
        }
    });
    Ok(())
}

/// Sanity: the HealthBook MergeMachine the integration relies on is the same one whose fold is a
/// pure function of the op set (mirrors the in-crate property test, but through the public API).
#[tokio::test]
async fn book_apply_keeps_latest_epoch_via_public_api() -> anyhow::Result<()> {
    let mut b = HealthBook::default();
    let v = SemVer::new(1, 0, 0);
    b.apply(HealthOp { writer: "n1".into(), health: health(3, Phase::Ready, v) });
    b.apply(HealthOp { writer: "n1".into(), health: health(1, Phase::Starting, v) }); // older -> ignored
    assert_eq!(b.get("n1").map(|h| h.epoch), Some(3));
    assert_eq!(b.get("n1").map(|h| h.phase), Some(Phase::Ready));
    Ok(())
}
