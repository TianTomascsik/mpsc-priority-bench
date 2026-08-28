/* VERSION WITHOUT REAL PAYLOAD SIZE
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::time::Duration;

use mpsc_priority_bench::{
    run_dual_bench, run_prio101_bench, BitmapQueue, DualSegQueue, MutexBinaryHeapQueue,
    SkiplistQueue,
};

/// One Criterion "iteration" performs OPS_PER_ITER end-to-end items:
/// - pushed by 20 producers
/// - popped by 1 consumer
///
/// Throughput is reported as elements/sec (ops/sec).
const OPS_PER_ITER: u64 = 200_000;

/// Your packet size sets (bytes).
const BENCH_SIZES_STREAM: &[usize] = &[64, 512, 1472, 4096, 16384, 65536, 1048576, 4194304];
const BENCH_SIZES_DGRAM: &[usize] = &[64, 512, 1472, 4096, 16384, 32768, 60000];

fn configure_group(group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    // Avoid "Unable to complete 100 samples in 5s" warnings:
    // - fewer samples
    // - longer measurement time
    group.sample_size(40);
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(10));
}

/// -------------------------
/// Baseline ops/sec benches
/// -------------------------
fn bench_dual_priority_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("dual_priority ops/s (HIGH/LOW), 20P:1C");
    configure_group(&mut group);

    group.throughput(Throughput::Elements(OPS_PER_ITER));

    group.bench_with_input(
        BenchmarkId::new("Impl1_MutexBinaryHeap", OPS_PER_ITER),
        &OPS_PER_ITER,
        |b, &ops_per_iter| {
            b.iter_custom(|iters| {
                let total_ops = iters * ops_per_iter;
                run_dual_bench::<MutexBinaryHeapQueue>(total_ops)
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("Impl2_DualSegQueue", OPS_PER_ITER),
        &OPS_PER_ITER,
        |b, &ops_per_iter| {
            b.iter_custom(|iters| {
                let total_ops = iters * ops_per_iter;
                run_dual_bench::<DualSegQueue>(total_ops)
            });
        },
    );

    group.finish();
}

fn bench_prio101_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("prio101 ops/s (0..=100), 20P:1C");
    configure_group(&mut group);

    group.throughput(Throughput::Elements(OPS_PER_ITER));

    group.bench_with_input(
        BenchmarkId::new("Impl3_Skiplist", OPS_PER_ITER),
        &OPS_PER_ITER,
        |b, &ops_per_iter| {
            b.iter_custom(|iters| {
                let total_ops = iters * ops_per_iter;
                run_prio101_bench::<SkiplistQueue>(total_ops)
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("Impl4_BitmapScanner", OPS_PER_ITER),
        &OPS_PER_ITER,
        |b, &ops_per_iter| {
            b.iter_custom(|iters| {
                let total_ops = iters * ops_per_iter;
                run_prio101_bench::<BitmapQueue>(total_ops)
            });
        },
    );

    group.finish();
}

/// ---------------------------------------------------------
/// Derived bytes/sec benches (packet size parameterization)
///
/// NOTE:
/// This does NOT change the actual queue work, because the queue payload is u64.
/// It simply reports "throughput in bytes/sec" assuming:
///   1 item == 1 packet of size `pkt_size`
/// So bytes/sec = items/sec * pkt_size.
/// ---------------------------------------------------------

fn bench_dual_priority_bytes_stream(c: &mut Criterion) {
    for &pkt_size in BENCH_SIZES_STREAM {
        let mut group = c.benchmark_group(format!(
            "dual_priority bytes/s STREAM (1 item == {} B), 20P:1C",
            pkt_size
        ));
        configure_group(&mut group);

        let bytes_per_iter = OPS_PER_ITER.saturating_mul(pkt_size as u64);
        group.throughput(Throughput::Bytes(bytes_per_iter));

        group.bench_with_input(
            BenchmarkId::new("Impl1_MutexBinaryHeap", pkt_size),
            &OPS_PER_ITER,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    let total_ops = iters * ops_per_iter;
                    run_dual_bench::<MutexBinaryHeapQueue>(total_ops)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Impl2_DualSegQueue", pkt_size),
            &OPS_PER_ITER,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    let total_ops = iters * ops_per_iter;
                    run_dual_bench::<DualSegQueue>(total_ops)
                });
            },
        );

        group.finish();
    }
}

fn bench_dual_priority_bytes_dgram(c: &mut Criterion) {
    for &pkt_size in BENCH_SIZES_DGRAM {
        let mut group = c.benchmark_group(format!(
            "dual_priority bytes/s DGRAM (1 item == {} B), 20P:1C",
            pkt_size
        ));
        configure_group(&mut group);

        let bytes_per_iter = OPS_PER_ITER.saturating_mul(pkt_size as u64);
        group.throughput(Throughput::Bytes(bytes_per_iter));

        group.bench_with_input(
            BenchmarkId::new("Impl1_MutexBinaryHeap", pkt_size),
            &OPS_PER_ITER,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    let total_ops = iters * ops_per_iter;
                    run_dual_bench::<MutexBinaryHeapQueue>(total_ops)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Impl2_DualSegQueue", pkt_size),
            &OPS_PER_ITER,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    let total_ops = iters * ops_per_iter;
                    run_dual_bench::<DualSegQueue>(total_ops)
                });
            },
        );

        group.finish();
    }
}

fn bench_prio101_bytes_stream(c: &mut Criterion) {
    for &pkt_size in BENCH_SIZES_STREAM {
        let mut group = c.benchmark_group(format!(
            "prio101 bytes/s STREAM (1 item == {} B), 20P:1C",
            pkt_size
        ));
        configure_group(&mut group);

        let bytes_per_iter = OPS_PER_ITER.saturating_mul(pkt_size as u64);
        group.throughput(Throughput::Bytes(bytes_per_iter));

        group.bench_with_input(
            BenchmarkId::new("Impl3_Skiplist", pkt_size),
            &OPS_PER_ITER,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    let total_ops = iters * ops_per_iter;
                    run_prio101_bench::<SkiplistQueue>(total_ops)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Impl4_BitmapScanner", pkt_size),
            &OPS_PER_ITER,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    let total_ops = iters * ops_per_iter;
                    run_prio101_bench::<BitmapQueue>(total_ops)
                });
            },
        );

        group.finish();
    }
}

fn bench_prio101_bytes_dgram(c: &mut Criterion) {
    for &pkt_size in BENCH_SIZES_DGRAM {
        let mut group = c.benchmark_group(format!(
            "prio101 bytes/s DGRAM (1 item == {} B), 20P:1C",
            pkt_size
        ));
        configure_group(&mut group);

        let bytes_per_iter = OPS_PER_ITER.saturating_mul(pkt_size as u64);
        group.throughput(Throughput::Bytes(bytes_per_iter));

        group.bench_with_input(
            BenchmarkId::new("Impl3_Skiplist", pkt_size),
            &OPS_PER_ITER,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    let total_ops = iters * ops_per_iter;
                    run_prio101_bench::<SkiplistQueue>(total_ops)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Impl4_BitmapScanner", pkt_size),
            &OPS_PER_ITER,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    let total_ops = iters * ops_per_iter;
                    run_prio101_bench::<BitmapQueue>(total_ops)
                });
            },
        );

        group.finish();
    }
}

/// Register all benchmark functions.
/// If this becomes too slow (lots of packet sizes), run with a filter:
///   cargo bench --bench mpsc_priority -- "prio101 bytes/s STREAM"
criterion_group!(
    benches,
    bench_dual_priority_ops,
    bench_prio101_ops,
    bench_dual_priority_bytes_stream,
    bench_dual_priority_bytes_dgram,
    bench_prio101_bytes_stream,
    bench_prio101_bytes_dgram
);
criterion_main!(benches);
*/

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::time::Duration;

use mpsc_priority_bench::{
    run_dual_bench_payload, run_prio101_bench_payload, BitmapQueue, DualSegQueue,
    MutexBinaryHeapQueue, SkiplistQueue,
};

const BENCH_SIZES_STREAM: &[usize] = &[64, 512, 1472, 4096, 16384, 65536, 1048576, 4194304];
const BENCH_SIZES_DGRAM: &[usize] = &[64, 512, 1472, 4096, 16384, 32768, 60000];

/// Keep “work per iteration” roughly constant across payload sizes by targeting a fixed byte budget.
/// This makes bytes/s comparisons more stable across sizes, and keeps runtime reasonable.
///
/// If you prefer constant message count per size, replace ops_for_size() with a fixed OPS_PER_ITER.
const TARGET_BYTES_PER_ITER: u64 = 64 * 1024 * 1024; // 64 MiB per Criterion "iteration"
const MIN_OPS_PER_ITER: u64 = 256;
const MAX_OPS_PER_ITER: u64 = 500_000;

fn ops_for_size(sz: usize) -> u64 {
    let raw = TARGET_BYTES_PER_ITER / (sz as u64);
    raw.clamp(MIN_OPS_PER_ITER, MAX_OPS_PER_ITER)
}

fn configure_group(group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>) {
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(3));
    group.measurement_time(Duration::from_secs(12));
}

/// -------------------------
/// Dual priority: STREAM
/// -------------------------
fn bench_dual_payload_stream_ops(c: &mut Criterion) {
    for &pkt_size in BENCH_SIZES_STREAM {
        let ops = ops_for_size(pkt_size);

        let mut group = c.benchmark_group(format!(
            "dual_priority payload STREAM ops/s (real bytes, {} B), 20P:1C",
            pkt_size
        ));
        configure_group(&mut group);
        group.throughput(Throughput::Elements(ops));

        group.bench_with_input(
            BenchmarkId::new("Impl1_MutexBinaryHeap", pkt_size),
            &ops,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    run_dual_bench_payload::<MutexBinaryHeapQueue>(iters * ops_per_iter, pkt_size)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Impl2_DualSegQueue", pkt_size),
            &ops,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    run_dual_bench_payload::<DualSegQueue>(iters * ops_per_iter, pkt_size)
                });
            },
        );

        group.finish();
    }
}

fn bench_dual_payload_stream_bytes(c: &mut Criterion) {
    for &pkt_size in BENCH_SIZES_STREAM {
        let ops = ops_for_size(pkt_size);

        let mut group = c.benchmark_group(format!(
            "dual_priority payload STREAM bytes/s (real bytes, {} B), 20P:1C",
            pkt_size
        ));
        configure_group(&mut group);
        group.throughput(Throughput::Bytes(ops * pkt_size as u64));

        group.bench_with_input(
            BenchmarkId::new("Impl1_MutexBinaryHeap", pkt_size),
            &ops,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    run_dual_bench_payload::<MutexBinaryHeapQueue>(iters * ops_per_iter, pkt_size)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Impl2_DualSegQueue", pkt_size),
            &ops,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    run_dual_bench_payload::<DualSegQueue>(iters * ops_per_iter, pkt_size)
                });
            },
        );

        group.finish();
    }
}

/// -------------------------
/// Dual priority: DGRAM
/// -------------------------
fn bench_dual_payload_dgram_ops(c: &mut Criterion) {
    for &pkt_size in BENCH_SIZES_DGRAM {
        let ops = ops_for_size(pkt_size);

        let mut group = c.benchmark_group(format!(
            "dual_priority payload DGRAM ops/s (real bytes, {} B), 20P:1C",
            pkt_size
        ));
        configure_group(&mut group);
        group.throughput(Throughput::Elements(ops));

        group.bench_with_input(
            BenchmarkId::new("Impl1_MutexBinaryHeap", pkt_size),
            &ops,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    run_dual_bench_payload::<MutexBinaryHeapQueue>(iters * ops_per_iter, pkt_size)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Impl2_DualSegQueue", pkt_size),
            &ops,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    run_dual_bench_payload::<DualSegQueue>(iters * ops_per_iter, pkt_size)
                });
            },
        );

        group.finish();
    }
}

fn bench_dual_payload_dgram_bytes(c: &mut Criterion) {
    for &pkt_size in BENCH_SIZES_DGRAM {
        let ops = ops_for_size(pkt_size);

        let mut group = c.benchmark_group(format!(
            "dual_priority payload DGRAM bytes/s (real bytes, {} B), 20P:1C",
            pkt_size
        ));
        configure_group(&mut group);
        group.throughput(Throughput::Bytes(ops * pkt_size as u64));

        group.bench_with_input(
            BenchmarkId::new("Impl1_MutexBinaryHeap", pkt_size),
            &ops,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    run_dual_bench_payload::<MutexBinaryHeapQueue>(iters * ops_per_iter, pkt_size)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Impl2_DualSegQueue", pkt_size),
            &ops,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    run_dual_bench_payload::<DualSegQueue>(iters * ops_per_iter, pkt_size)
                });
            },
        );

        group.finish();
    }
}

/// -------------------------
/// Prio101: STREAM
/// -------------------------
fn bench_prio101_payload_stream_ops(c: &mut Criterion) {
    for &pkt_size in BENCH_SIZES_STREAM {
        let ops = ops_for_size(pkt_size);

        let mut group = c.benchmark_group(format!(
            "prio101 payload STREAM ops/s (real bytes, {} B), 20P:1C",
            pkt_size
        ));
        configure_group(&mut group);
        group.throughput(Throughput::Elements(ops));

        group.bench_with_input(
            BenchmarkId::new("Impl3_Skiplist", pkt_size),
            &ops,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    run_prio101_bench_payload::<SkiplistQueue>(iters * ops_per_iter, pkt_size)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Impl4_BitmapScanner", pkt_size),
            &ops,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    run_prio101_bench_payload::<BitmapQueue>(iters * ops_per_iter, pkt_size)
                });
            },
        );

        group.finish();
    }
}

fn bench_prio101_payload_stream_bytes(c: &mut Criterion) {
    for &pkt_size in BENCH_SIZES_STREAM {
        let ops = ops_for_size(pkt_size);

        let mut group = c.benchmark_group(format!(
            "prio101 payload STREAM bytes/s (real bytes, {} B), 20P:1C",
            pkt_size
        ));
        configure_group(&mut group);
        group.throughput(Throughput::Bytes(ops * pkt_size as u64));

        group.bench_with_input(
            BenchmarkId::new("Impl3_Skiplist", pkt_size),
            &ops,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    run_prio101_bench_payload::<SkiplistQueue>(iters * ops_per_iter, pkt_size)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Impl4_BitmapScanner", pkt_size),
            &ops,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    run_prio101_bench_payload::<BitmapQueue>(iters * ops_per_iter, pkt_size)
                });
            },
        );

        group.finish();
    }
}

/// -------------------------
/// Prio101: DGRAM
/// -------------------------
fn bench_prio101_payload_dgram_ops(c: &mut Criterion) {
    for &pkt_size in BENCH_SIZES_DGRAM {
        let ops = ops_for_size(pkt_size);

        let mut group = c.benchmark_group(format!(
            "prio101 payload DGRAM ops/s (real bytes, {} B), 20P:1C",
            pkt_size
        ));
        configure_group(&mut group);
        group.throughput(Throughput::Elements(ops));

        group.bench_with_input(
            BenchmarkId::new("Impl3_Skiplist", pkt_size),
            &ops,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    run_prio101_bench_payload::<SkiplistQueue>(iters * ops_per_iter, pkt_size)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Impl4_BitmapScanner", pkt_size),
            &ops,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    run_prio101_bench_payload::<BitmapQueue>(iters * ops_per_iter, pkt_size)
                });
            },
        );

        group.finish();
    }
}

fn bench_prio101_payload_dgram_bytes(c: &mut Criterion) {
    for &pkt_size in BENCH_SIZES_DGRAM {
        let ops = ops_for_size(pkt_size);

        let mut group = c.benchmark_group(format!(
            "prio101 payload DGRAM bytes/s (real bytes, {} B), 20P:1C",
            pkt_size
        ));
        configure_group(&mut group);
        group.throughput(Throughput::Bytes(ops * pkt_size as u64));

        group.bench_with_input(
            BenchmarkId::new("Impl3_Skiplist", pkt_size),
            &ops,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    run_prio101_bench_payload::<SkiplistQueue>(iters * ops_per_iter, pkt_size)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("Impl4_BitmapScanner", pkt_size),
            &ops,
            |b, &ops_per_iter| {
                b.iter_custom(|iters| {
                    run_prio101_bench_payload::<BitmapQueue>(iters * ops_per_iter, pkt_size)
                });
            },
        );

        group.finish();
    }
}

criterion_group!(
    benches,
    bench_dual_payload_stream_ops,
    bench_dual_payload_stream_bytes,
    bench_dual_payload_dgram_ops,
    bench_dual_payload_dgram_bytes,
    bench_prio101_payload_stream_ops,
    bench_prio101_payload_stream_bytes,
    bench_prio101_payload_dgram_ops,
    bench_prio101_payload_dgram_bytes
);
criterion_main!(benches);
