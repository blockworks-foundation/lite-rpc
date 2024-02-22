use crate::structures::produced_block::ProducedBlockInner;
use itertools::Itertools;
use log::{debug, trace};
use solana_lite_rpc_util::statistics::percentiles::calculate_percentiles;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Instant;
use tracing::debug_span;

lazy_static::lazy_static! {
    static ref ARC_PRODUCED_BLOCK: Mutex<Vec<(std::sync::Weak<ProducedBlockInner>, Instant)>> =
        Mutex::new(Vec::with_capacity(1000));
}

pub fn track_producedblock_allocation(new_arc: &Arc<ProducedBlockInner>) {
    let _span = debug_span!("track_producedblock_allocation").entered();
    let weak: std::sync::Weak<ProducedBlockInner> = Arc::downgrade(new_arc);
    ARC_PRODUCED_BLOCK
        .lock()
        .unwrap()
        .push((weak, Instant::now()));
}

pub fn start_produced_block_inspect_task() -> JoinHandle<()> {
    std::thread::spawn(move || loop {
        std::thread::sleep(std::time::Duration::from_secs(10));
        produced_block_inspect_refs();
    })
}

fn produced_block_inspect_refs() {
    let mut references = ARC_PRODUCED_BLOCK.lock().unwrap();

    let mut live = 0;
    let mut freed = 0;
    for r in references.iter() {
        trace!(
            "- {} refs, created at {:?}",
            r.0.strong_count(),
            r.1.elapsed()
        );
        if r.0.strong_count() == 0 {
            freed += 1;
        } else {
            live += 1;
        }
    }

    if freed >= 100 {
        references.retain(|r| r.0.strong_count() > 0);
    }

    let dist = references
        .iter()
        .filter(|r| r.0.strong_count() > 0)
        .map(|r| r.1.elapsed().as_secs_f64() * 1000.0)
        .sorted_by(|a, b| a.partial_cmp(b).unwrap())
        .collect::<Vec<f64>>();

    let percentiles = calculate_percentiles(&dist);
    trace!(
        "debug refs helt on ProducedBlock Arc - percentiles of time_ms: {}",
        percentiles
    );
    debug!(
        "refs helt on ProducedBlock: live: {}, freed: {}, p50={:.1}ms, p95={:.1}ms, max={:.1}ms",
        live,
        freed,
        percentiles.get_bucket_value(0.50).unwrap(),
        percentiles.get_bucket_value(0.95).unwrap(),
        percentiles.get_bucket_value(1.0).unwrap(),
    );
}
