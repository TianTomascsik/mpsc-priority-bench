use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::time::Duration;

use mpsc_priority_bench::{
    run_dual_bench_payload, run_prio101_bench_payload, BitmapQueue, DualSegQueue,
    MutexBinaryHeapQueue, SkiplistQueue,
};

const BENCH_SIZES_STREAM: &[usize] = &[64, 512, 1472, 4096, 16384, 65536, 1048576, 4194304];
const BENCH_SIZES_DGRAM: &[usize] = &[64, 512, 1472, 4096, 16384, 32768, 60000];

// Keep "work per iteration" roughly constant across payload sizes by targeting
// a fixed byte budget: bytes/s comparisons stay stable across sizes and the
// total runtime stays reasonable.
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

// -------------------------
// Dual priority: STREAM
// -------------------------
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

// -------------------------
// Dual priority: DGRAM
// -------------------------
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

// -------------------------
// Prio101: STREAM
// -------------------------
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

// -------------------------
// Prio101: DGRAM
// -------------------------
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
