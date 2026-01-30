use kv_db::db::KvDB;
use kv_db::store::DbTraits;
use rand::{Rng, distributions::Alphanumeric};
use std::{
    fs,
    time::{Duration, Instant},
};

const DB_DIR: &str = "./tmp/rust_db";
const VALUE_SIZE: usize = 1024; // 1KB
const DURATION_SECS: u64 = 60; // run benchmark for 60 seconds
const PROGRESS_OPS: u64 = 100_000; // print progress every 100k ops

fn main() {
    // Clean previous DB directory
    let _ = fs::remove_dir_all(DB_DIR);
    fs::create_dir_all(DB_DIR).unwrap();

    // Open your KV store
    let mut store = KvDB::open(DB_DIR).expect("Failed to open KV store");

    println!("Starting Rust KV Store PUT benchmark...");

    // Prepare the value buffer
    let value = vec![0u8; VALUE_SIZE];
    let mut rng = rand::thread_rng();

    let mut ops: u64 = 0;
    let mut latencies: Vec<Duration> = Vec::with_capacity(5_000_000);

    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(DURATION_SECS) {
        let key = random_key(&mut rng, 16);

        let t0 = Instant::now();
        store.put(key.as_bytes(), &value).unwrap();
        latencies.push(t0.elapsed());

        ops += 1;

        if ops % PROGRESS_OPS == 0 {
            println!("Completed {} ops...", ops);
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    report_results("PUT", ops, &latencies, elapsed);
}

// Generate random alphanumeric string key
fn random_key<R: Rng>(rng: &mut R, len: usize) -> String {
    rng.sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

// Report metrics
fn report_results(op: &str, ops: u64, latencies: &[Duration], elapsed: f64) {
    let mut lats = latencies.to_vec();
    lats.sort_unstable();

    let ops_per_sec = ops as f64 / elapsed;
    let p50 = percentile(&lats, 50);
    let p95 = percentile(&lats, 95);
    let p99 = percentile(&lats, 99);

    println!("\n===== Benchmark Results =====");
    println!("Operation: {}", op);
    println!("Total Ops: {}", ops);
    println!("Elapsed time: {:.2} sec", elapsed);
    println!("Ops/sec: {:.0}", ops_per_sec);
    println!("p50 latency: {:?}", p50);
    println!("p95 latency: {:?}", p95);
    println!("p99 latency: {:?}", p99);
    println!("=============================");
}

// Percentile calculation
fn percentile(lats: &[Duration], pct: usize) -> Duration {
    if lats.is_empty() {
        return Duration::from_secs(0);
    }
    let mut idx = lats.len() * pct / 100;
    if idx >= lats.len() {
        idx = lats.len() - 1;
    }
    lats[idx]
}
