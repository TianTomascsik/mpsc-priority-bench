#!/usr/bin/env python3
"""
criterion_insights.py

Reads Criterion.rs benchmark output under target/criterion and generates:
- analysis_out/records.csv: all parsed measurements
- analysis_out/insights.txt: rankings + speedups
- analysis_out/plots/*.png: per-scenario plots
- analysis_out/plots/overview_all_absolute.png: one big "everything" overview
- analysis_out/plots/overview_all_normalized.png: normalized overview (shape comparisons)

Usage:
  python3 criterion_insights.py
  python3 criterion_insights.py --root target/criterion --out analysis_out
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

import matplotlib.pyplot as plt


# ----------------------------
# Data model
# ----------------------------

@dataclass
class Record:
    criterion_root: str
    rel_case_dir: str

    group: str
    workload: str              # dual_priority | prio101 | ""
    transport: str             # STREAM | DGRAM | ""
    has_payload_word: bool
    payload_size_bytes: Optional[int]

    impl: str
    param: Optional[str]

    # nanoseconds
    mean_ns: float
    lower_ns: float
    upper_ns: float

    throughput_kind: str       # Bytes | Elements | ""
    throughput_value: Optional[float]

    # derived
    ops_per_s: Optional[float]
    bytes_per_s: Optional[float]
    gib_per_s: Optional[float]
    ns_per_op: Optional[float]


# ----------------------------
# Parsing helpers
# ----------------------------

_WORKLOAD_RE = re.compile(r"^(dual_priority|prio101)\b", re.IGNORECASE)
_TRANSPORT_RE = re.compile(r"\b(STREAM|DGRAM)\b", re.IGNORECASE)
_PAYLOAD_SIZE_RE = re.compile(r"(\d+)\s*B\b")
_NUMERIC_RE = re.compile(r"^\d+$")


def safe_float(x: Any) -> Optional[float]:
    try:
        return float(x)
    except Exception:
        return None


def parse_estimates(estimates_path: Path) -> Tuple[float, float, float]:
    """Return (mean_ns, lower_ns, upper_ns) from estimates.json."""
    with estimates_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    mean = data.get("mean", {})
    ci = mean.get("confidence_interval", {})
    mean_ns = float(mean.get("point_estimate"))
    lower_ns = float(ci.get("lower_bound"))
    upper_ns = float(ci.get("upper_bound"))
    return mean_ns, lower_ns, upper_ns


def parse_benchmark_throughput(case_dir: Path) -> Tuple[str, Optional[float]]:
    """
    Read throughput from benchmark.json if present.
    Usually encoded like {"throughput": {"Bytes": N}} or {"throughput": {"Elements": N}}.
    """
    candidates = [
        case_dir / "benchmark.json",
        case_dir / "new" / "benchmark.json",
        case_dir / "base" / "benchmark.json",
    ]
    for p in candidates:
        if p.exists():
            with p.open("r", encoding="utf-8") as f:
                data = json.load(f)

            thr = data.get("throughput", None)
            if thr is None:
                return ("", None)

            if isinstance(thr, dict):
                if "Bytes" in thr:
                    return ("Bytes", safe_float(thr["Bytes"]))
                if "Elements" in thr:
                    return ("Elements", safe_float(thr["Elements"]))
            return ("", None)

    return ("", None)


def parse_group_metadata(group: str) -> Tuple[str, str, bool, Optional[int]]:
    """Return workload, transport, has_payload_word, payload_size_bytes (best-effort from group string)."""
    workload = ""
    m = _WORKLOAD_RE.search(group)
    if m:
        workload = m.group(1).lower()

    transport = ""
    tm = _TRANSPORT_RE.search(group)
    if tm:
        transport = tm.group(1).upper()

    has_payload_word = "payload" in group.lower()

    sizes = [int(x) for x in _PAYLOAD_SIZE_RE.findall(group)]
    payload_size_bytes = sizes[-1] if sizes else None

    return workload, transport, has_payload_word, payload_size_bytes


def parse_case_identity(criterion_root: Path, case_dir: Path) -> Tuple[str, str, str, Optional[str]]:
    """
    Infer (group, impl, rel_case_dir, param) from directory structure:

      <group>/<ImplName>/<param>/new/estimates.json

    group = first component
    impl = second component (if not numeric)
    param = last component (often numeric)
    """
    rel = case_dir.relative_to(criterion_root)
    parts = list(rel.parts)
    group = parts[0] if parts else ""

    impl = ""
    param = None

    if len(parts) == 1:
        impl = "default"
    elif len(parts) >= 2:
        if _NUMERIC_RE.match(parts[1]):
            impl = "default"
            param = parts[1]
        else:
            impl = parts[1]
            if len(parts) >= 3:
                param = parts[-1]

    return group, impl, str(rel), param


def derive_rates(
    mean_ns: float,
    throughput_kind: str,
    throughput_value: Optional[float],
) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """Return (ops_per_s, bytes_per_s, gib_per_s, ns_per_op) derived from time and throughput."""
    if throughput_kind == "" or throughput_value is None:
        return (None, None, None, None)

    mean_s = mean_ns / 1e9
    if mean_s <= 0:
        return (None, None, None, None)

    if throughput_kind == "Elements":
        ops_s = throughput_value / mean_s
        ns_per_op = (mean_ns / throughput_value) if throughput_value > 0 else None
        return (ops_s, None, None, ns_per_op)

    if throughput_kind == "Bytes":
        bps = throughput_value / mean_s
        gib_s = bps / (1024.0 ** 3)
        return (None, bps, gib_s, None)

    return (None, None, None, None)


# ----------------------------
# Plotting helpers
# ----------------------------

def slug(s: str) -> str:
    s = s.strip().lower()
    s = re.sub(r"[^\w]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "plot"


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def plot_lines_by_size(
    records: List[Record],
    out_dir: Path,
    title: str,
    y_key: str,
    y_label: str,
    file_name: str,
    use_log_x: bool = True,
    use_log_y: bool = False,
) -> None:
    rows = [r for r in records if r.payload_size_bytes is not None and getattr(r, y_key) is not None]
    if not rows:
        return

    by_impl: Dict[str, List[Record]] = defaultdict(list)
    for r in rows:
        by_impl[r.impl].append(r)

    plt.figure(figsize=(10, 6))
    for impl, rs in sorted(by_impl.items(), key=lambda x: x[0]):
        rs_sorted = sorted(rs, key=lambda z: z.payload_size_bytes or 0)
        xs = [z.payload_size_bytes for z in rs_sorted]
        ys = [getattr(z, y_key) for z in rs_sorted]
        plt.plot(xs, ys, marker="o", label=impl)

    plt.title(title)
    plt.xlabel("payload size (bytes)")
    plt.ylabel(y_label)
    plt.legend()

    if use_log_x:
        plt.xscale("log", base=2)
    if use_log_y:
        plt.yscale("log")

    plt.tight_layout()
    plt.savefig(out_dir / file_name, dpi=160)
    plt.close()


def plot_bar(
    records: List[Record],
    out_dir: Path,
    title: str,
    y_key: str,
    y_label: str,
    file_name: str,
) -> None:
    rows = [r for r in records if getattr(r, y_key) is not None]
    if not rows:
        return

    best: Dict[str, float] = {}
    for r in rows:
        v = float(getattr(r, y_key))
        best[r.impl] = max(best.get(r.impl, float("-inf")), v)

    impls = sorted(best.keys())
    vals = [best[i] for i in impls]

    plt.figure(figsize=(10, 6))
    plt.bar(impls, vals)
    plt.title(title)
    plt.xlabel("implementation")
    plt.ylabel(y_label)
    plt.xticks(rotation=25, ha="right")
    plt.tight_layout()
    plt.savefig(out_dir / file_name, dpi=160)
    plt.close()


# ----------------------------
# One big overview plot (2x2)
# ----------------------------

def _series_key(r: Record) -> str:
    """
    Distinguish STREAM vs DGRAM in the legend.
    """
    t = r.transport or "NA"
    return f"{r.impl} [{t}]"


def plot_overview_all(records: List[Record], plots_dir: Path) -> None:
    """
    Creates:
      - overview_all_absolute.png: absolute ops/s and GiB/s
      - overview_all_normalized.png: each series normalized to its own max (shape comparison)

    Layout (2x2):
      [0,0] dual_priority Elements (ops/s)
      [0,1] dual_priority Bytes (GiB/s)
      [1,0] prio101 Elements (ops/s)
      [1,1] prio101 Bytes (GiB/s)
    """
    # Only consider size-dependent series (payload_size_bytes present)
    sized = [r for r in records if r.payload_size_bytes is not None and r.throughput_kind in ("Elements", "Bytes")]
    if not sized:
        return

    def select(workload: str, thr_kind: str) -> List[Record]:
        return [r for r in sized if r.workload == workload and r.throughput_kind == thr_kind]

    panels = [
        ("dual_priority", "Elements", "Ops/s", "ops_per_s"),
        ("dual_priority", "Bytes", "GiB/s", "gib_per_s"),
        ("prio101", "Elements", "Ops/s", "ops_per_s"),
        ("prio101", "Bytes", "GiB/s", "gib_per_s"),
    ]

    def draw(fig_path: Path, normalize: bool) -> None:
        fig, axes = plt.subplots(2, 2, figsize=(20, 12))
        axes = axes.reshape(2, 2)

        for idx, (wl, kind, y_label, y_key) in enumerate(panels):
            ax = axes[idx // 2][idx % 2]
            rs = select(wl, kind)
            rs = [r for r in rs if getattr(r, y_key) is not None]
            if not rs:
                ax.set_axis_off()
                continue

            by_series: Dict[str, List[Record]] = defaultdict(list)
            for r in rs:
                by_series[_series_key(r)].append(r)

            # Sort series names for stable legend
            for series_name in sorted(by_series.keys()):
                srs = sorted(by_series[series_name], key=lambda z: z.payload_size_bytes or 0)
                xs = [z.payload_size_bytes for z in srs]
                ys_raw = [float(getattr(z, y_key)) for z in srs]

                if normalize:
                    denom = max(ys_raw) if ys_raw else 0.0
                    ys = [(y / denom) if denom > 0 else 0.0 for y in ys_raw]
                else:
                    ys = ys_raw

                # Line style for transport (STREAM solid, DGRAM dashed, NA dotted)
                transport = (srs[0].transport or "NA").upper()
                if transport == "STREAM":
                    ls = "-"
                elif transport == "DGRAM":
                    ls = "--"
                else:
                    ls = ":"

                ax.plot(xs, ys, marker="o", linestyle=ls, label=series_name)

            ax.set_title(f"{wl} | {kind} ({'normalized' if normalize else 'absolute'})")
            ax.set_xlabel("payload size (bytes)")
            ax.set_ylabel(y_label if not normalize else f"{y_label} (norm. to max)")
            ax.set_xscale("log", base=2)
            ax.grid(True, which="both", linewidth=0.5, alpha=0.4)
            ax.legend(fontsize="small", ncol=2)

        fig.suptitle("Criterion overview (all implementations, STREAM vs DGRAM)", fontsize=16)
        fig.tight_layout(rect=[0, 0, 1, 0.96])
        fig.savefig(fig_path, dpi=180)
        plt.close(fig)

    draw(plots_dir / "overview_all_absolute.png", normalize=False)
    draw(plots_dir / "overview_all_normalized.png", normalize=True)


# ----------------------------
# Insights
# ----------------------------

def geometric_mean(values: List[float]) -> Optional[float]:
    vals = [v for v in values if v is not None and v > 0]
    if not vals:
        return None
    return math.exp(sum(math.log(v) for v in vals) / len(vals))


def scenario_key(r: Record) -> Tuple[str, str, bool, str]:
    return (r.workload, r.transport, r.has_payload_word, r.throughput_kind)


def make_insights(records: List[Record]) -> str:
    out: List[str] = []
    out.append("Criterion insights summary")
    out.append("==========================")
    out.append("")
    out.append(f"Total parsed records: {len(records)}")
    out.append("")

    by_scn: Dict[Tuple[str, str, bool, str], List[Record]] = defaultdict(list)
    for r in records:
        if r.throughput_kind:
            by_scn[scenario_key(r)].append(r)

    for scn, rs in sorted(by_scn.items(), key=lambda x: x[0]):
        workload, transport, has_payload, thr_kind = scn
        label = f"Scenario: workload={workload or 'unknown'} transport={transport or 'n/a'} payload_word={has_payload} metric={thr_kind}"
        out.append(label)
        out.append("-" * len(label))

        by_impl: Dict[str, List[Record]] = defaultdict(list)
        for r in rs:
            by_impl[r.impl].append(r)

        scores: List[Tuple[str, float]] = []
        for impl, rlist in by_impl.items():
            if thr_kind == "Elements":
                vals = [x.ops_per_s for x in rlist if x.ops_per_s is not None]
            else:
                vals = [x.gib_per_s for x in rlist if x.gib_per_s is not None]

            gm = geometric_mean([v for v in vals if v is not None])
            if gm is None:
                gm = max(vals) if vals else None
            if gm is not None:
                scores.append((impl, float(gm)))

        scores.sort(key=lambda x: x[1], reverse=True)
        if not scores:
            out.append("No throughput values available.\n")
            continue

        best_impl, best_score = scores[0]
        out.append("Implementation ranking (geometric mean):")
        for impl, sc in scores:
            rel = (sc / best_score) if best_score > 0 else float("nan")
            out.append(f"  - {impl:24s}  score={sc:.4g}  rel_to_best={rel:.3f}")
        out.append("")

        if len(scores) >= 2:
            second_impl, second_score = scores[1]
            speedup = best_score / second_score if second_score > 0 else float("inf")
            out.append(f"Top-1 vs Top-2 speedup: {best_impl} / {second_impl} = {speedup:.3f}x")
            out.append("")

    return "\n".join(out)


# ----------------------------
# IO and scanning
# ----------------------------

def scan_records(criterion_root: Path, root_label: str = "target/criterion") -> List[Record]:
    # root_label is what gets recorded in the CSV: the root as the user gave it
    # (repo-relative by default), never the resolved absolute path.
    records: List[Record] = []

    if not criterion_root.exists():
        raise FileNotFoundError(f"Criterion root not found: {criterion_root}")

    estimate_files = list(criterion_root.glob("**/new/estimates.json"))
    if not estimate_files:
        estimate_files = list(criterion_root.glob("**/estimates.json"))

    for est_path in estimate_files:
        if est_path.name != "estimates.json":
            continue

        case_dir = est_path.parent.parent if est_path.parent.name == "new" else est_path.parent
        group, impl, rel_case_dir, param = parse_case_identity(criterion_root, case_dir)
        workload, transport, has_payload_word, payload_size_bytes = parse_group_metadata(group)

        try:
            mean_ns, lower_ns, upper_ns = parse_estimates(est_path)
        except Exception as e:
            print(f"[WARN] failed to parse {est_path}: {e}", file=sys.stderr)
            continue

        thr_kind, thr_val = parse_benchmark_throughput(case_dir)
        ops_s, bytes_s, gib_s, ns_per_op = derive_rates(mean_ns, thr_kind, thr_val)

        records.append(
            Record(
                criterion_root=root_label,
                rel_case_dir=rel_case_dir,
                group=group,
                workload=workload,
                transport=transport,
                has_payload_word=has_payload_word,
                payload_size_bytes=payload_size_bytes,
                impl=impl,
                param=param,
                mean_ns=mean_ns,
                lower_ns=lower_ns,
                upper_ns=upper_ns,
                throughput_kind=thr_kind,
                throughput_value=thr_val,
                ops_per_s=ops_s,
                bytes_per_s=bytes_s,
                gib_per_s=gib_s,
                ns_per_op=ns_per_op,
            )
        )

    return records


def write_csv(records: List[Record], out_path: Path) -> None:
    ensure_dir(out_path.parent)
    if not records:
        return
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(asdict(records[0]).keys()))
        w.writeheader()
        for r in records:
            w.writerow(asdict(r))


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=str, default="target/criterion", help="Criterion output dir (default: target/criterion)")
    ap.add_argument("--out", type=str, default="analysis_out", help="Output directory (default: analysis_out)")
    args = ap.parse_args()

    criterion_root = Path(args.root).resolve()
    out_dir = Path(args.out).resolve()
    plots_dir = out_dir / "plots"
    ensure_dir(plots_dir)

    records = scan_records(criterion_root, root_label=args.root)
    if not records:
        print(f"No records found under {criterion_root}. Expected Criterion output like **/new/estimates.json.", file=sys.stderr)
        return 2

    write_csv(records, out_dir / "records.csv")

    insights = make_insights(records)
    ensure_dir(out_dir)
    (out_dir / "insights.txt").write_text(insights, encoding="utf-8")
    print(insights)

    # Per-scenario plots
    by_scn: Dict[Tuple[str, str, bool, str], List[Record]] = defaultdict(list)
    for r in records:
        if r.throughput_kind:
            by_scn[scenario_key(r)].append(r)

    for scn, rs in by_scn.items():
        workload, transport, has_payload, thr_kind = scn
        has_sizes = any(r.payload_size_bytes is not None for r in rs)
        base_title = f"{workload or 'unknown'} | {transport or 'n/a'} | payload_word={has_payload} | {thr_kind}"
        fname_base = slug(base_title)

        if thr_kind == "Bytes":
            if has_sizes:
                plot_lines_by_size(
                    rs, plots_dir,
                    title=f"Throughput (GiB/s) vs payload size\n{base_title}",
                    y_key="gib_per_s",
                    y_label="GiB/s",
                    file_name=f"{fname_base}_bytes_gib_s_vs_size.png",
                    use_log_x=True,
                    use_log_y=False,
                )
            else:
                plot_bar(
                    rs, plots_dir,
                    title=f"Best Throughput (GiB/s)\n{base_title}",
                    y_key="gib_per_s",
                    y_label="GiB/s",
                    file_name=f"{fname_base}_bytes_gib_s_bar.png",
                )

        elif thr_kind == "Elements":
            if has_sizes:
                plot_lines_by_size(
                    rs, plots_dir,
                    title=f"Ops/s vs payload size\n{base_title}",
                    y_key="ops_per_s",
                    y_label="ops/s",
                    file_name=f"{fname_base}_ops_s_vs_size.png",
                    use_log_x=True,
                    use_log_y=False,
                )
                plot_lines_by_size(
                    rs, plots_dir,
                    title=f"Latency (ns/op) vs payload size\n{base_title}",
                    y_key="ns_per_op",
                    y_label="ns/op",
                    file_name=f"{fname_base}_latency_ns_per_op_vs_size.png",
                    use_log_x=True,
                    use_log_y=False,
                )
            else:
                plot_bar(
                    rs, plots_dir,
                    title=f"Best Ops/s\n{base_title}",
                    y_key="ops_per_s",
                    y_label="ops/s",
                    file_name=f"{fname_base}_ops_s_bar.png",
                )

    # One big overview plot (absolute + normalized)
    plot_overview_all(records, plots_dir)

    print("")
    print(f"Wrote: {out_dir / 'records.csv'}")
    print(f"Wrote: {out_dir / 'insights.txt'}")
    print(f"Wrote plots under: {plots_dir}")
    print(f"Overview: {plots_dir / 'overview_all_absolute.png'}")
    print(f"Overview: {plots_dir / 'overview_all_normalized.png'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
