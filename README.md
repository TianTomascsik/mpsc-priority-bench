# mpsc_priority_bench

A benchmark study of **multi-producer / single-consumer priority queue
disciplines** under heavy contention (20 producers : 1 consumer), built to
answer one design question: *is an in-process priority queue worth it on a
network gateway's hot path, or should traffic classes get dedicated workers
instead?* The study was conducted as part of a master's thesis on a
transparent encryption gateway, to decide its internal queueing architecture.

## The two workloads (each a fixed pairwise comparison)

| Workload | Priorities | Implementation pair |
|---|---|---|
| `dual_priority` | 2 classes (HIGH/LOW) | `Impl1_MutexBinaryHeap` (mutex + binary heap baseline) vs `Impl2_DualSegQueue` (two lock-free `SegQueue`s, HIGH drained first) |
| `prio101` | 101 bounded levels (0–100) | `Impl3_Skiplist` (`crossbeam-skiplist` ordered map) vs `Impl4_BitmapScanner` (per-level `SegQueue`s + atomic non-empty bitmap, MSB scan) |

**Every comparison is pairwise within one workload — never a 4-way ranking.**
The two workloads answer different questions (2 classes vs 101 levels), and
each implementation was written for its workload's contract.

## The two payload modes

- **No payload** (`has_payload_word=false`): each item moves one machine word.
  This isolates the queue discipline itself.
- **Real payload** (`has_payload_word=true`): each item references a
  producer-owned slot of real bytes (64 B – 4 MiB) that the producer writes
  and the consumer reads (one byte per cache line + the last byte), so memory
  traffic scales with message size like a real gateway relay.

## Headline results (from the committed `analysis_out/`)

- Without payload copies, the two-class segregated queue sustains **4.0×**
  the rate of the mutex-protected binary heap (median 83 vs 333 ns per
  operation under 20-producer contention), and at 101 levels the
  bitmap-scanned level array beats the lock-free skiplist by **8.6×**.
- With real payload copies the gap shrinks as the payload grows, reaching
  **1.0–1.1×** (two-class) and **1.0×** (101-level) at **4 MiB**: memory
  traffic, not the queue discipline, dominates the hot path at large message
  sizes. Full payload-sweep spans: 1.0–8.8× (two-class), 1.0–3.1× (101-level).

This is why the gateway ultimately **reserves workers per traffic class
instead of queueing**: the discipline's advantage evaporates exactly where
the data plane operates (large buffers).

## Running it

```bash
cargo test          # correctness tests for all four queue implementations
cargo bench         # the Criterion study (long; writes target/criterion/)

# Parse Criterion output into analysis_out/ (records.csv + plots; needs matplotlib)
python3 criterion_insights.py

# Emit the citation-checked summary block (stdlib-only)
python3 criterion_thesis_summary.py
```

`criterion_insights.py` requires **Python 3 + matplotlib**;
`criterion_thesis_summary.py` is stdlib-only.

## What is committed and where it came from

`analysis_out/` (records.csv, plots/, insights.txt, thesis_caption.txt) is
the durable record of the study: regenerating it requires a multi-hour
`cargo bench` run, so the parsed results are committed. The committed data
was measured on the thesis evaluation host — **AMD Ryzen 9 5950X (16C/32T),
Arch Linux**, with the 20-producer/1-consumer topology described above.
Numbers on different hardware will differ; the *shape* of the results (the
payload-size collapse) is the transferable finding.

## Methodology limits

- MPSC only, one consumer thread; no work-stealing or multi-consumer designs.
- Pairwise comparisons within a workload only (see above).
- The payload mode measures a produce-write / consume-touch pattern, not a
  full protocol stack; it brackets memory-traffic effects, nothing more.
- Criterion wall-clock benches on a desktop OS: no core isolation beyond
  Criterion's own warm-up/outlier handling.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.
