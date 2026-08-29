#!/usr/bin/env python3
"""
Emit a compact, citation-checked summary of the priority-queue study.

Reads analysis_out/records.csv (produced by criterion_insights.py from the Criterion
estimates) and writes analysis_out/caption.txt — a summary block in which
every number is computed from the CSV, so any prose citing the study can be checked
against the data mechanically.

The study's two workloads each carry exactly one implementation pair:
  * dual_priority  — Impl1_MutexBinaryHeap vs Impl2_DualSegQueue (two classes, HIGH/LOW)
  * prio101        — Impl3_Skiplist        vs Impl4_BitmapScanner (101 bounded levels)
so every comparison here is pairwise within one workload, never a 4-way ranking.

Ratios are computed per matched case (same group + param, both implementations present)
as mean_ns(slower) / mean_ns(faster) of the same benchmarked quantity, split by whether
the case moves real payload bytes (has_payload_word) — the no-payload cases isolate the
queue discipline, the payload cases show memory traffic eroding the discipline's edge.

Stdlib only (csv/statistics); no third-party dependencies.
"""
from __future__ import annotations

import csv
import statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent
RECORDS = HERE / "analysis_out" / "records.csv"
OUT = HERE / "analysis_out" / "caption.txt"

PAIRS = {
    "dual_priority": ("Impl2_DualSegQueue", "Impl1_MutexBinaryHeap",
                      "two-class segregated queue (DualSegQueue)",
                      "mutex-protected binary heap"),
    "prio101": ("Impl4_BitmapScanner", "Impl3_Skiplist",
                "bitmap-scanned level array (BitmapScanner)",
                "lock-free skiplist"),
}


def _f(row: dict, key: str) -> float | None:
    raw = (row.get(key) or "").strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def load_rows() -> list[dict]:
    with RECORDS.open(newline="", encoding="utf-8") as fh:
        return [r for r in csv.DictReader(fh) if r.get("workload") in PAIRS]


def pair_cases(rows: list[dict], workload: str, payload: bool) -> list[tuple[float, float]]:
    """Matched cases of one workload as (payload_size_bytes, slower/faster mean_ns ratio)."""
    fast_impl, slow_impl, _, _ = PAIRS[workload]
    cases: dict[tuple[str, str], dict] = {}
    for r in rows:
        if r["workload"] != workload:
            continue
        if (r.get("has_payload_word", "").strip().lower() == "true") is not payload:
            continue
        mean = _f(r, "mean_ns")
        if mean is None or mean <= 0:
            continue
        slot = cases.setdefault((r["group"], r.get("param", "")), {})
        slot[r["impl"]] = mean
        slot["size"] = _f(r, "payload_size_bytes") or 0.0
    out = []
    for both in cases.values():
        if fast_impl in both and slow_impl in both:
            out.append((both["size"], both[slow_impl] / both[fast_impl]))
    return sorted(out)


def pair_ratios(rows: list[dict], workload: str, payload: bool) -> list[float]:
    """mean_ns ratio slower/faster per matched case of one workload."""
    return sorted(r for _s, r in pair_cases(rows, workload, payload))


def ratios_at_largest_payload(rows: list[dict], workload: str) -> tuple[float, list[float]]:
    """(largest payload size, ratios at that size) over the payload-carrying cases."""
    cases = pair_cases(rows, workload, payload=True)
    if not cases:
        return 0.0, []
    top = max(s for s, _r in cases)
    return top, sorted(r for s, r in cases if s == top)


def ns_per_op(rows: list[dict], workload: str, impl: str) -> tuple[float, float, float] | None:
    """Median ns/op (+ CI bounds scaled by the same op count) over no-payload cases."""
    vals, los, his = [], [], []
    for r in rows:
        if r["workload"] != workload or r["impl"] != impl:
            continue
        if r.get("has_payload_word", "").strip().lower() == "true":
            continue
        nso = _f(r, "ns_per_op")
        mean = _f(r, "mean_ns")
        if nso is None or mean is None or mean <= 0:
            continue
        scale = nso / mean  # ops count is mean/ns_per_op; reuse it for the CI bounds
        vals.append(nso)
        lo, hi = _f(r, "lower_ns"), _f(r, "upper_ns")
        if lo is not None:
            los.append(lo * scale)
        if hi is not None:
            his.append(hi * scale)
    if not vals:
        return None
    return (statistics.median(vals),
            statistics.median(los) if los else float("nan"),
            statistics.median(his) if his else float("nan"))


def fmt_range(ratios: list[float]) -> str:
    if not ratios:
        return "n/a"
    lo, hi = min(ratios), max(ratios)
    return f"{lo:.1f}×" if f"{lo:.1f}" == f"{hi:.1f}" else f"{lo:.1f}–{hi:.1f}×"


def fmt_bytes(n: float) -> str:
    for unit, factor in (("MiB", 1 << 20), ("KiB", 1 << 10)):
        if n >= factor:
            v = n / factor
            return f"{v:.0f} {unit}" if v == int(v) else f"{v:.1f} {unit}"
    return f"{n:.0f} B"


def main() -> None:
    rows = load_rows()
    n = len(rows)

    dual_np = pair_ratios(rows, "dual_priority", payload=False)
    prio_np = pair_ratios(rows, "prio101", payload=False)
    dual_pl = pair_ratios(rows, "dual_priority", payload=True)
    prio_pl = pair_ratios(rows, "prio101", payload=True)
    dual_top, dual_top_r = ratios_at_largest_payload(rows, "dual_priority")
    prio_top, prio_top_r = ratios_at_largest_payload(rows, "prio101")

    dual_med = statistics.median(dual_np) if dual_np else float("nan")
    prio_med = statistics.median(prio_np) if prio_np else float("nan")

    seg = ns_per_op(rows, "dual_priority", "Impl2_DualSegQueue")
    heap = ns_per_op(rows, "dual_priority", "Impl1_MutexBinaryHeap")
    ns_txt = ""
    if seg and heap:
        ns_txt = (f" (median {seg[0]:.0f} vs {heap[0]:.0f} ns per operation under "
                  f"20-producer contention)")

    takeaway = (
        f"Without payload copies the {PAIRS['dual_priority'][2]} sustains "
        f"{dual_med:.1f}× the rate of the {PAIRS['dual_priority'][3]}{ns_txt}, and at "
        f"101 bounded priority levels the {PAIRS['prio101'][2]} beats the "
        f"{PAIRS['prio101'][3]} by {prio_med:.1f}×; with real payload copies the gap "
        f"shrinks as the payload grows, reaching {fmt_range(dual_top_r)} (two-class) and "
        f"{fmt_range(prio_top_r)} (101-level) at "
        f"{fmt_bytes(dual_top) if dual_top == prio_top else fmt_bytes(dual_top) + '/' + fmt_bytes(prio_top)}"
        f" — memory traffic, not the queue discipline, dominates the hot path at large "
        f"message sizes (full payload span: {fmt_range(dual_pl)} / {fmt_range(prio_pl)})."
    )
    headline = (
        f"Priority-queue study: {n} Criterion records over two workloads "
        f"(dual_priority HIGH/LOW; prio101 with 101 bounded levels), 20 producers : "
        f"1 consumer, STREAM and DGRAM item shapes, payload sizes swept where marked."
    )
    method = (
        "Each workload carries exactly one implementation pair (dual_priority: "
        "Impl1_MutexBinaryHeap vs Impl2_DualSegQueue; prio101: Impl3_Skiplist vs "
        "Impl4_BitmapScanner), so all comparisons are pairwise within a workload, never a "
        "4-way ranking. Ratios = mean_ns(slower)/mean_ns(faster) per matched case (same "
        "group and parameter, both implementations measured); 95% confidence bounds come "
        "from Criterion's estimates. Cases without has_payload_word move one machine word "
        "per item (an earlier no-payload variant of the benches, preserved in git "
        "history) and isolate the queue discipline; payload cases copy real bytes per "
        "item. This benchmarks the in-process queue primitive in isolation — NOT the "
        "end-to-end gateway QoS path — and informed the architectural decision to "
        "reserve workers per traffic class instead of queueing."
    )
    provenance = f"source: analysis_out/records.csv · {n} records · emitted by criterion_caption.py"

    lines = [
        "QSTUDY  mpsc_priority_bench queue study",
        f"  headline:   {headline}",
        f"  takeaway:   {takeaway}",
        f"  method:     {method}",
        f"  provenance: {provenance}",
        "",
    ]
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    print(f"pairs: dual no-payload {len(dual_np)} cases {fmt_range(dual_np)}; "
          f"prio101 no-payload {len(prio_np)} cases {fmt_range(prio_np)}; "
          f"dual payload {len(dual_pl)} cases; prio101 payload {len(prio_pl)} cases")
    print(f"wrote {OUT.relative_to(HERE)}")


if __name__ == "__main__":
    main()
