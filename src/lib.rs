/* VERSION WITHOUT REAL PAYLOAD SIZE
use crossbeam::queue::SegQueue;
use crossbeam_skiplist::SkipMap;

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub const PRODUCERS: usize = 20;

/// ---------------------------
/// Small deterministic PRNG
/// ---------------------------
#[inline(always)]
fn xorshift64(state: &mut u64) -> u64 {
    // Classic xorshift64
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// ---------------------------
/// Category A: Dual Priority
/// ---------------------------
pub const LOW: u8 = 0;
pub const HIGH: u8 = 1;

/// Dual-priority queue interface (MPSC, single consumer).
pub trait DualPrioQueue: Send + Sync + 'static {
    fn push(&self, prio: u8, val: u64);
    fn pop(&self) -> Option<u64>;
}

/// Impl 1: Mutex<BinaryHeap<...>> baseline with (Priority, SequenceNum, Value)
///
/// BinaryHeap is a max-heap. We want:
/// - higher priority first (HIGH > LOW)
/// - FIFO within the same priority => smaller sequence first
///
/// So for equal priority, we treat a *smaller* seq as "greater" in heap ordering.
#[derive(Debug)]
struct HeapItem {
    prio: u8,
    seq: u64,
    val: u64,
}

impl Eq for HeapItem {}
impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.prio == other.prio && self.seq == other.seq && self.val == other.val
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.prio.cmp(&other.prio) {
            Ordering::Equal => {
                // Reverse seq ordering to make smaller seq pop first in a max-heap.
                other.seq.cmp(&self.seq)
            }
            ord => ord,
        }
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct HeapInner {
    seq: u64,
    heap: BinaryHeap<HeapItem>,
}

pub struct MutexBinaryHeapQueue {
    inner: Mutex<HeapInner>,
}

impl Default for MutexBinaryHeapQueue {
    fn default() -> Self {
        Self {
            inner: Mutex::new(HeapInner {
                seq: 0,
                heap: BinaryHeap::new(),
            }),
        }
    }
}

impl DualPrioQueue for MutexBinaryHeapQueue {
    #[inline(always)]
    fn push(&self, prio: u8, val: u64) {
        let mut g = self.inner.lock().unwrap();
        let seq = g.seq;
        g.seq = seq.wrapping_add(1);
        g.heap.push(HeapItem { prio, seq, val });
    }

    #[inline(always)]
    fn pop(&self) -> Option<u64> {
        let mut g = self.inner.lock().unwrap();
        g.heap.pop().map(|it| it.val)
    }
}

/// Impl 2: Optimized bucket queue = two SegQueue instances (High + Low).
///
/// Consumer logic:
/// - try HIGH first, else LOW
pub struct DualSegQueue {
    high: SegQueue<u64>,
    low: SegQueue<u64>,
}

impl Default for DualSegQueue {
    fn default() -> Self {
        Self {
            high: SegQueue::new(),
            low: SegQueue::new(),
        }
    }
}

impl DualPrioQueue for DualSegQueue {
    #[inline(always)]
    fn push(&self, prio: u8, val: u64) {
        if prio == HIGH {
            self.high.push(val);
        } else {
            self.low.push(val);
        }
    }

    #[inline(always)]
    fn pop(&self) -> Option<u64> {
        if let Some(v) = self.high.pop() {
            return Some(v);
        }
        self.low.pop()
    }
}

/// ---------------------------
/// Category B: 101 Priorities (0..=100)
/// ---------------------------
pub const MAX_PRIO: usize = 100;
pub const NUM_PRIOS: usize = MAX_PRIO + 1;

/// 0..=100 priority queue interface (MPSC, single consumer).
pub trait Prio101Queue: Send + Sync + 'static {
    fn push(&self, prio: u8, val: u64);
    fn pop(&self) -> Option<u64>; // must return highest-priority available item
}

/// Impl 3: Skiplist baseline using crossbeam-skiplist.
///
/// Key choice:
/// - We want highest priority first => higher `prio` should sort later.
/// - FIFO within same prio => earlier sequence first.
///   We use Reverse(seq) so that smaller seq sorts *later* for a given prio,
///   enabling `iter().next_back()` to return earliest-inserted for the top prio.
///
/// Key = (prio, Reverse(seq))
pub struct SkiplistQueue {
    seq: AtomicU64,
    map: SkipMap<(u8, Reverse<u64>), u64>,
}

impl Default for SkiplistQueue {
    fn default() -> Self {
        Self {
            seq: AtomicU64::new(0),
            map: SkipMap::new(),
        }
    }
}

impl Prio101Queue for SkiplistQueue {
    #[inline(always)]
    fn push(&self, prio: u8, val: u64) {
        let seq = self.seq.fetch_add(1, AtomicOrdering::Relaxed);
        self.map.insert((prio, Reverse(seq)), val);
    }

    #[inline(always)]
    fn pop(&self) -> Option<u64> {
        // Single-consumer: safe to do "get key then remove key" without another remover racing.
        let entry = self.map.iter().next_back()?;
        let key = entry.key().clone();
        let val = *entry.value();
        self.map.remove(&key);
        Some(val)
    }
}

/// Impl 4: Optimized bitmap scanner:
/// - Vec<SegQueue> size 101
/// - Atomic bitmask (2 x AtomicU64) marks which priorities are non-empty
///
/// Producer:
///   queues[prio].push(val);
///   set bit(prio)
///
/// Consumer:
///   - load mask (Acquire)
///   - find highest set bit using CPU bit scans (leading_zeros => MSB index)
///   - try to pop that queue
///
/// Important race-handling detail:
/// - Consumer may observe bit set but queue temporarily empty (or becomes empty).
/// - When pop() returns None for a priority chosen from the mask, the consumer:
///     1) clears the bit
///     2) immediately re-checks pop()
///        - if it finds an item, it re-sets the bit before returning
///
/// This prevents "lost work" where the bit is cleared while an item exists, which
/// could otherwise stall if producers stop.
pub struct BitmapQueue {
    queues: Vec<SegQueue<u64>>,
    mask_lo: std::sync::atomic::AtomicU64, // prio 0..63
    mask_hi: std::sync::atomic::AtomicU64, // prio 64..127 (we use up to 100)
}

impl Default for BitmapQueue {
    fn default() -> Self {
        let queues = (0..NUM_PRIOS).map(|_| SegQueue::new()).collect();
        Self {
            queues,
            mask_lo: std::sync::atomic::AtomicU64::new(0),
            mask_hi: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl BitmapQueue {
    #[inline(always)]
    fn set_bit(&self, prio: u8) {
        if prio < 64 {
            let bit = 1u64 << prio;
            self.mask_lo.fetch_or(bit, AtomicOrdering::Release);
        } else {
            let bit = 1u64 << (prio - 64);
            self.mask_hi.fetch_or(bit, AtomicOrdering::Release);
        }
    }

    #[inline(always)]
    fn clear_bit(&self, prio: u8) {
        if prio < 64 {
            let bit = 1u64 << prio;
            self.mask_lo.fetch_and(!bit, AtomicOrdering::AcqRel);
        } else {
            let bit = 1u64 << (prio - 64);
            self.mask_hi.fetch_and(!bit, AtomicOrdering::AcqRel);
        }
    }

    #[inline(always)]
    fn highest_prio_from_mask(&self) -> Option<u8> {
        // Mask off invalid bits above MAX_PRIO in the hi word.
        // prio 64..=100 => hi bits 0..=36 (37 bits).
        const HI_BITS: u32 = (NUM_PRIOS as u32) - 64; // 101-64 = 37
        const HI_VALID_MASK: u64 = (1u64 << HI_BITS) - 1;

        let hi = self.mask_hi.load(AtomicOrdering::Acquire) & HI_VALID_MASK;
        if hi != 0 {
            // Find MSB index: 63 - leading_zeros
            let idx = 63u8 - (hi.leading_zeros() as u8);
            return Some(64 + idx);
        }

        let lo = self.mask_lo.load(AtomicOrdering::Acquire);
        if lo != 0 {
            let idx = 63u8 - (lo.leading_zeros() as u8);
            return Some(idx);
        }

        None
    }
}

impl Prio101Queue for BitmapQueue {
    #[inline(always)]
    fn push(&self, prio: u8, val: u64) {
        self.queues[prio as usize].push(val);
        self.set_bit(prio);
    }

    #[inline(always)]
    fn pop(&self) -> Option<u64> {
        loop {
            let prio = self.highest_prio_from_mask()?;
            let q = &self.queues[prio as usize];

            if let Some(v) = q.pop() {
                // Do not clear bit here; we may still have more items.
                // If it becomes empty, we'll discover that on a later pop attempt.
                return Some(v);
            }

            // Queue empty (at least at this instant). Clear the bit to avoid rescanning.
            self.clear_bit(prio);

            // Immediately re-check to avoid losing an item if it became visible / arrived
            // around the clear operation. If we observe an item, re-set the bit.
            if let Some(v) = q.pop() {
                self.set_bit(prio);
                return Some(v);
            }

            // Otherwise, continue scanning.
            std::hint::spin_loop();
        }
    }
}

/// ---------------------------
/// Benchmark harness
/// ---------------------------

/// Runs a dual-priority benchmark for exactly `total_ops` pushed+consumed items.
pub fn run_dual_bench<Q: DualPrioQueue + Default>(total_ops: u64) -> Duration {
    let q = Arc::new(Q::default());
    let barrier = Arc::new(Barrier::new(PRODUCERS + 1 + 1)); // producers + consumer + main
    let start_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let consumer_q = Arc::clone(&q);
    let consumer_barrier = Arc::clone(&barrier);
    let consumer_start = Arc::clone(&start_flag);

    let consumer = thread::spawn(move || {
        consumer_barrier.wait();
        while !consumer_start.load(AtomicOrdering::Acquire) {
            std::hint::spin_loop();
        }

        let mut consumed = 0u64;
        while consumed < total_ops {
            if let Some(v) = consumer_q.pop() {
                // Prevent accidental "dead code elimination" of the queue work.
                std::hint::black_box(v);
                consumed += 1;
            } else {
                std::hint::spin_loop();
            }
        }
    });

    // Distribute exact work across producers (no global "produced" counter contention).
    let base = total_ops / (PRODUCERS as u64);
    let rem = total_ops % (PRODUCERS as u64);

    let mut producers = Vec::with_capacity(PRODUCERS);
    for i in 0..PRODUCERS {
        let producer_q = Arc::clone(&q);
        let producer_barrier = Arc::clone(&barrier);
        let producer_start = Arc::clone(&start_flag);

        let my_ops = base + if (i as u64) < rem { 1 } else { 0 };
        let seed = 0x9E3779B97F4A7C15u64 ^ ((i as u64 + 1) * 0xD1B54A32D192ED03u64);

        producers.push(thread::spawn(move || {
            producer_barrier.wait();
            while !producer_start.load(AtomicOrdering::Acquire) {
                std::hint::spin_loop();
            }

            let mut s = seed;
            for n in 0..my_ops {
                let r = xorshift64(&mut s);
                let prio = if (r & 1) == 0 { HIGH } else { LOW };
                producer_q.push(prio, n);
            }
        }));
    }

    // Align start of measurement with simultaneous release of all worker threads.
    barrier.wait();
    let start = Instant::now();
    start_flag.store(true, AtomicOrdering::Release);

    for p in producers {
        p.join().unwrap();
    }
    consumer.join().unwrap();

    start.elapsed()
}

/// Runs a 0..=100 priority benchmark for exactly `total_ops` pushed+consumed items.
pub fn run_prio101_bench<Q: Prio101Queue + Default>(total_ops: u64) -> Duration {
    let q = Arc::new(Q::default());
    let barrier = Arc::new(Barrier::new(PRODUCERS + 1 + 1)); // producers + consumer + main
    let start_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let consumer_q = Arc::clone(&q);
    let consumer_barrier = Arc::clone(&barrier);
    let consumer_start = Arc::clone(&start_flag);

    let consumer = thread::spawn(move || {
        consumer_barrier.wait();
        while !consumer_start.load(AtomicOrdering::Acquire) {
            std::hint::spin_loop();
        }

        let mut consumed = 0u64;
        while consumed < total_ops {
            if let Some(v) = consumer_q.pop() {
                std::hint::black_box(v);
                consumed += 1;
            } else {
                std::hint::spin_loop();
            }
        }
    });

    let base = total_ops / (PRODUCERS as u64);
    let rem = total_ops % (PRODUCERS as u64);

    let mut producers = Vec::with_capacity(PRODUCERS);
    for i in 0..PRODUCERS {
        let producer_q = Arc::clone(&q);
        let producer_barrier = Arc::clone(&barrier);
        let producer_start = Arc::clone(&start_flag);

        let my_ops = base + if (i as u64) < rem { 1 } else { 0 };
        let seed = 0xC2B2AE3D27D4EB4Fu64 ^ ((i as u64 + 1) * 0x165667B19E3779F9u64);

        producers.push(thread::spawn(move || {
            producer_barrier.wait();
            while !producer_start.load(AtomicOrdering::Acquire) {
                std::hint::spin_loop();
            }

            let mut s = seed;
            for n in 0..my_ops {
                let r = xorshift64(&mut s);
                let prio = (r % (NUM_PRIOS as u64)) as u8; // 0..=100
                producer_q.push(prio, n);
            }
        }));
    }

    barrier.wait();
    let start = Instant::now();
    start_flag.store(true, AtomicOrdering::Release);

    for p in producers {
        p.join().unwrap();
    }
    consumer.join().unwrap();

    start.elapsed()
}
*/

use crossbeam::queue::SegQueue;
use crossbeam_skiplist::SkipMap;

use std::cell::UnsafeCell;
use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering as AtomicOrdering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

pub const PRODUCERS: usize = 20;

pub const LOW: u8 = 0;
pub const HIGH: u8 = 1;

pub const MAX_PRIO: usize = 100;
pub const NUM_PRIOS: usize = MAX_PRIO + 1;

// ---------------------------
// Small deterministic PRNG
// ---------------------------
#[inline(always)]
fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

// ---------------------------
// Payload pools (size-real)
// ---------------------------

/// Message enqueued into the priority queues.
/// Actual bytes live in the producer-local pool, referenced by (producer, slot).
#[derive(Clone, Copy, Debug)]
pub struct Msg {
    pub producer: u8,
    pub slot: u16,
}

/// Producer-owned pool: `slots` fixed-size payload buffers of `payload_size` bytes.
/// Producer writes into its slots; consumer reads and releases slots.
/// Synchronization is via `free[slot]` atomics.
pub struct ProducerPool {
    payload_size: usize,
    slots: usize,

    // One big contiguous backing store: slots * payload_size.
    // We use UnsafeCell so we can write through shared references (Arc),
    // with safety ensured by the per-slot `free` atomic state machine.
    storage: Box<[UnsafeCell<u8>]>,

    // 1 = free, 0 = in-use
    free: Vec<AtomicU8>,
}

// SAFETY: the only non-Sync field is `storage` (UnsafeCell<u8> backing store).
// All cross-thread access to a slot's bytes is serialised by the per-slot
// `free` AtomicU8 state machine plus the queue handoff:
// - a producer may write a slot only after winning the 1->0 CAS in
//   `acquire_slot` (AcqRel), giving it exclusive ownership of those bytes;
// - the consumer reads the slot only after popping the Msg from a queue,
//   whose internal synchronisation orders the pop after the producer's push
//   (which happens after the payload write on the same thread);
// - `release_slot` publishes with a Release store before the slot can be
//   re-acquired (Acquire side of the CAS).
// So no two threads ever access the same slot bytes unsynchronised.
unsafe impl Send for ProducerPool {}
// SAFETY: see the Send justification above; &ProducerPool only permits slot
// access through the same atomic ownership protocol.
unsafe impl Sync for ProducerPool {}

impl ProducerPool {
    pub fn new(slots: usize, payload_size: usize) -> Self {
        assert!(payload_size > 0);
        assert!(slots > 0);

        let total = slots
            .checked_mul(payload_size)
            .expect("slots * payload_size overflow");

        let storage: Vec<UnsafeCell<u8>> = (0..total).map(|_| UnsafeCell::new(0u8)).collect();
        let free: Vec<AtomicU8> = (0..slots).map(|_| AtomicU8::new(1)).collect();

        Self {
            payload_size,
            slots,
            storage: storage.into_boxed_slice(),
            free,
        }
    }

    #[inline(always)]
    fn base_ptr(&self) -> *mut u8 {
        // UnsafeCell<u8> is repr(transparent) over u8, so this cast is valid.
        self.storage.as_ptr() as *mut u8
    }

    #[inline(always)]
    pub fn acquire_slot(&self, rr_hint: &mut usize) -> u16 {
        loop {
            // Try `slots` times starting from rr_hint to avoid hot-spotting slot 0.
            for _ in 0..self.slots {
                let s = *rr_hint;
                *rr_hint += 1;
                if *rr_hint == self.slots {
                    *rr_hint = 0;
                }

                if self.free[s]
                    .compare_exchange(1, 0, AtomicOrdering::AcqRel, AtomicOrdering::Relaxed)
                    .is_ok()
                {
                    return s as u16;
                }
            }
            std::hint::spin_loop();
        }
    }

    #[inline(always)]
    pub fn release_slot(&self, slot: u16) {
        self.free[slot as usize].store(1, AtomicOrdering::Release);
    }

    /// Producer writes the full payload (size-real write traffic).
    #[inline(always)]
    pub fn write_payload(&self, slot: u16, pattern: u8) {
        let off = (slot as usize) * self.payload_size;
        let ptr = self.base_ptr();
        // SAFETY: `slot < self.slots` (acquire_slot only returns such values),
        // so `off + payload_size <= slots * payload_size = storage.len()` —
        // the write stays in bounds. Exclusive access to these bytes is held
        // by the caller, who owns the slot via the acquire_slot CAS.
        unsafe {
            std::ptr::write_bytes(ptr.add(off), pattern, self.payload_size);
        }
    }

    /// Consumer reads/touches the payload (size-real read traffic).
    /// We touch one byte per cache line to make scaling realistic without a full byte-wise scan.
    #[inline(always)]
    pub fn touch_payload(&self, slot: u16) -> u64 {
        let off = (slot as usize) * self.payload_size;
        let ptr = self.base_ptr() as *const u8;

        let mut acc: u64 = 0;
        let mut i = 0usize;
        while i < self.payload_size {
            // SAFETY: `i < payload_size` and `slot < slots`, so
            // `off + i < storage.len()`; the caller owns the slot (received
            // via a queue pop), so the read does not race a writer.
            unsafe {
                acc ^= *ptr.add(off + i) as u64;
            }
            i += 64; // one byte per cache line
        }
        // Also touch the last byte.
        // SAFETY: `payload_size > 0` (asserted in `new`), so
        // `off + payload_size - 1` is the slot's last in-bounds byte; same
        // ownership argument as above.
        unsafe {
            acc ^= *ptr.add(off + self.payload_size - 1) as u64;
        }
        acc
    }
}

// ---------------------------
// Category A: Dual Priority
// ---------------------------

pub trait DualPrioQueue: Send + Sync + 'static {
    fn push(&self, prio: u8, msg: Msg);
    fn pop(&self) -> Option<Msg>;
}

/// Impl 1: Mutex<BinaryHeap<...>> baseline with (Priority, SequenceNum, Msg)
#[derive(Debug)]
struct HeapItem {
    prio: u8,
    seq: u64,
    msg: Msg,
}

impl Eq for HeapItem {}
impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.prio == other.prio
            && self.seq == other.seq
            && self.msg.producer == other.msg.producer
            && self.msg.slot == other.msg.slot
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.prio.cmp(&other.prio) {
            Ordering::Equal => {
                // Reverse seq ordering to make smaller seq pop first in a max-heap.
                other.seq.cmp(&self.seq)
            }
            ord => ord,
        }
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

struct HeapInner {
    seq: u64,
    heap: BinaryHeap<HeapItem>,
}

pub struct MutexBinaryHeapQueue {
    inner: Mutex<HeapInner>,
}

impl Default for MutexBinaryHeapQueue {
    fn default() -> Self {
        Self {
            inner: Mutex::new(HeapInner {
                seq: 0,
                heap: BinaryHeap::new(),
            }),
        }
    }
}

impl DualPrioQueue for MutexBinaryHeapQueue {
    #[inline(always)]
    fn push(&self, prio: u8, msg: Msg) {
        let mut g = self.inner.lock().unwrap();
        let seq = g.seq;
        g.seq = seq.wrapping_add(1);
        g.heap.push(HeapItem { prio, seq, msg });
    }

    #[inline(always)]
    fn pop(&self) -> Option<Msg> {
        let mut g = self.inner.lock().unwrap();
        g.heap.pop().map(|it| it.msg)
    }
}

/// Impl 2: Two SegQueue instances (High + Low)
pub struct DualSegQueue {
    high: SegQueue<Msg>,
    low: SegQueue<Msg>,
}

impl Default for DualSegQueue {
    fn default() -> Self {
        Self {
            high: SegQueue::new(),
            low: SegQueue::new(),
        }
    }
}

impl DualPrioQueue for DualSegQueue {
    #[inline(always)]
    fn push(&self, prio: u8, msg: Msg) {
        if prio == HIGH {
            self.high.push(msg);
        } else {
            self.low.push(msg);
        }
    }

    #[inline(always)]
    fn pop(&self) -> Option<Msg> {
        if let Some(m) = self.high.pop() {
            return Some(m);
        }
        self.low.pop()
    }
}

// ---------------------------
// Category B: 101 Priorities (0..=100)
// ---------------------------

pub trait Prio101Queue: Send + Sync + 'static {
    fn push(&self, prio: u8, msg: Msg);
    fn pop(&self) -> Option<Msg>;
}

/// Impl 3: Skiplist baseline.
/// Key = (prio, Reverse(seq)) so that iter().next_back() returns:
/// - highest priority
/// - earliest sequence within that priority (FIFO)
pub struct SkiplistQueue {
    seq: AtomicU64,
    map: SkipMap<(u8, Reverse<u64>), Msg>,
}

impl Default for SkiplistQueue {
    fn default() -> Self {
        Self {
            seq: AtomicU64::new(0),
            map: SkipMap::new(),
        }
    }
}

impl Prio101Queue for SkiplistQueue {
    #[inline(always)]
    fn push(&self, prio: u8, msg: Msg) {
        let seq = self.seq.fetch_add(1, AtomicOrdering::Relaxed);
        self.map.insert((prio, Reverse(seq)), msg);
    }

    #[inline(always)]
    fn pop(&self) -> Option<Msg> {
        let entry = self.map.iter().next_back()?;
        let key = *entry.key();
        let val = *entry.value();
        self.map.remove(&key);
        Some(val)
    }
}

/// Impl 4: Bitmap scanner + array of queues
pub struct BitmapQueue {
    queues: Vec<SegQueue<Msg>>,
    mask_lo: std::sync::atomic::AtomicU64, // prio 0..63
    mask_hi: std::sync::atomic::AtomicU64, // prio 64..127 (we use up to 100)
}

impl Default for BitmapQueue {
    fn default() -> Self {
        let queues = (0..NUM_PRIOS).map(|_| SegQueue::new()).collect();
        Self {
            queues,
            mask_lo: std::sync::atomic::AtomicU64::new(0),
            mask_hi: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

impl BitmapQueue {
    #[inline(always)]
    fn set_bit(&self, prio: u8) {
        if prio < 64 {
            let bit = 1u64 << prio;
            self.mask_lo.fetch_or(bit, AtomicOrdering::Release);
        } else {
            let bit = 1u64 << (prio - 64);
            self.mask_hi.fetch_or(bit, AtomicOrdering::Release);
        }
    }

    #[inline(always)]
    fn clear_bit(&self, prio: u8) {
        if prio < 64 {
            let bit = 1u64 << prio;
            self.mask_lo.fetch_and(!bit, AtomicOrdering::AcqRel);
        } else {
            let bit = 1u64 << (prio - 64);
            self.mask_hi.fetch_and(!bit, AtomicOrdering::AcqRel);
        }
    }

    #[inline(always)]
    fn highest_prio_from_mask(&self) -> Option<u8> {
        const HI_BITS: u32 = (NUM_PRIOS as u32) - 64; // 101 - 64 = 37
        const HI_VALID_MASK: u64 = (1u64 << HI_BITS) - 1;

        let hi = self.mask_hi.load(AtomicOrdering::Acquire) & HI_VALID_MASK;
        if hi != 0 {
            let idx = 63u8 - (hi.leading_zeros() as u8);
            return Some(64 + idx);
        }

        let lo = self.mask_lo.load(AtomicOrdering::Acquire);
        if lo != 0 {
            let idx = 63u8 - (lo.leading_zeros() as u8);
            return Some(idx);
        }

        None
    }
}

impl Prio101Queue for BitmapQueue {
    #[inline(always)]
    fn push(&self, prio: u8, msg: Msg) {
        self.queues[prio as usize].push(msg);
        self.set_bit(prio);
    }

    #[inline(always)]
    fn pop(&self) -> Option<Msg> {
        loop {
            let prio = self.highest_prio_from_mask()?;
            let q = &self.queues[prio as usize];

            if let Some(m) = q.pop() {
                return Some(m);
            }

            self.clear_bit(prio);

            if let Some(m) = q.pop() {
                self.set_bit(prio);
                return Some(m);
            }

            std::hint::spin_loop();
        }
    }
}

// ---------------------------
// Benchmark harness (payload-size-real)
// ---------------------------

fn slots_per_producer_for_size(payload_size: usize) -> usize {
    if payload_size <= 4 * 1024 {
        64
    } else if payload_size <= 64 * 1024 {
        16
    } else if payload_size <= 1024 * 1024 {
        8
    } else {
        4
    }
}

/// Dual-priority benchmark with real payload writes/reads.
pub fn run_dual_bench_payload<Q: DualPrioQueue + Default>(
    total_ops: u64,
    payload_size: usize,
) -> Duration {
    let q = Arc::new(Q::default());

    let slots = slots_per_producer_for_size(payload_size);
    let pools: Arc<Vec<ProducerPool>> = Arc::new(
        (0..PRODUCERS)
            .map(|_| ProducerPool::new(slots, payload_size))
            .collect(),
    );

    let barrier = Arc::new(Barrier::new(PRODUCERS + 1 + 1));
    let start_flag = Arc::new(AtomicBool::new(false));

    // Consumer
    let consumer_q = Arc::clone(&q);
    let consumer_pools = Arc::clone(&pools);
    let consumer_barrier = Arc::clone(&barrier);
    let consumer_start = Arc::clone(&start_flag);

    let consumer = thread::spawn(move || {
        consumer_barrier.wait();
        while !consumer_start.load(AtomicOrdering::Acquire) {
            std::hint::spin_loop();
        }

        let mut consumed = 0u64;
        let mut acc: u64 = 0;

        while consumed < total_ops {
            if let Some(m) = consumer_q.pop() {
                let pool = &consumer_pools[m.producer as usize];
                acc ^= pool.touch_payload(m.slot);
                pool.release_slot(m.slot);

                consumed += 1;
            } else {
                std::hint::spin_loop();
            }
        }

        std::hint::black_box(acc);
    });

    // Producers
    let base = total_ops / (PRODUCERS as u64);
    let rem = total_ops % (PRODUCERS as u64);

    let mut producers = Vec::with_capacity(PRODUCERS);
    for i in 0..PRODUCERS {
        let producer_q = Arc::clone(&q);
        let producer_pools = Arc::clone(&pools);
        let producer_barrier = Arc::clone(&barrier);
        let producer_start = Arc::clone(&start_flag);

        let my_ops = base + if (i as u64) < rem { 1 } else { 0 };
        let seed = 0x9E3779B97F4A7C15u64 ^ ((i as u64 + 1) * 0xD1B54A32D192ED03u64);

        producers.push(thread::spawn(move || {
            let pool = &producer_pools[i];
            let mut rr = 0usize;

            producer_barrier.wait();
            while !producer_start.load(AtomicOrdering::Acquire) {
                std::hint::spin_loop();
            }

            let mut s = seed;
            for n in 0..my_ops {
                let r = xorshift64(&mut s);
                let prio = if (r & 1) == 0 { HIGH } else { LOW };

                let slot = pool.acquire_slot(&mut rr);
                pool.write_payload(slot, (n as u8).wrapping_add(i as u8));

                producer_q.push(
                    prio,
                    Msg {
                        producer: i as u8,
                        slot,
                    },
                );
            }
        }));
    }

    barrier.wait();
    let start = Instant::now();
    start_flag.store(true, AtomicOrdering::Release);

    for p in producers {
        p.join().unwrap();
    }
    consumer.join().unwrap();

    start.elapsed()
}

/// 0..=100 priority benchmark with real payload writes/reads.
pub fn run_prio101_bench_payload<Q: Prio101Queue + Default>(
    total_ops: u64,
    payload_size: usize,
) -> Duration {
    let q = Arc::new(Q::default());

    let slots = slots_per_producer_for_size(payload_size);
    let pools: Arc<Vec<ProducerPool>> = Arc::new(
        (0..PRODUCERS)
            .map(|_| ProducerPool::new(slots, payload_size))
            .collect(),
    );

    let barrier = Arc::new(Barrier::new(PRODUCERS + 1 + 1));
    let start_flag = Arc::new(AtomicBool::new(false));

    // Consumer
    let consumer_q = Arc::clone(&q);
    let consumer_pools = Arc::clone(&pools);
    let consumer_barrier = Arc::clone(&barrier);
    let consumer_start = Arc::clone(&start_flag);

    let consumer = thread::spawn(move || {
        consumer_barrier.wait();
        while !consumer_start.load(AtomicOrdering::Acquire) {
            std::hint::spin_loop();
        }

        let mut consumed = 0u64;
        let mut acc: u64 = 0;

        while consumed < total_ops {
            if let Some(m) = consumer_q.pop() {
                let pool = &consumer_pools[m.producer as usize];
                acc ^= pool.touch_payload(m.slot);
                pool.release_slot(m.slot);

                consumed += 1;
            } else {
                std::hint::spin_loop();
            }
        }

        std::hint::black_box(acc);
    });

    // Producers
    let base = total_ops / (PRODUCERS as u64);
    let rem = total_ops % (PRODUCERS as u64);

    let mut producers = Vec::with_capacity(PRODUCERS);
    for i in 0..PRODUCERS {
        let producer_q = Arc::clone(&q);
        let producer_pools = Arc::clone(&pools);
        let producer_barrier = Arc::clone(&barrier);
        let producer_start = Arc::clone(&start_flag);

        let my_ops = base + if (i as u64) < rem { 1 } else { 0 };
        let seed = 0xC2B2AE3D27D4EB4Fu64 ^ ((i as u64 + 1) * 0x165667B19E3779F9u64);

        producers.push(thread::spawn(move || {
            let pool = &producer_pools[i];
            let mut rr = 0usize;

            producer_barrier.wait();
            while !producer_start.load(AtomicOrdering::Acquire) {
                std::hint::spin_loop();
            }

            let mut s = seed;
            for n in 0..my_ops {
                let r = xorshift64(&mut s);
                let prio = (r % (NUM_PRIOS as u64)) as u8;

                let slot = pool.acquire_slot(&mut rr);
                pool.write_payload(slot, (n as u8).wrapping_add(i as u8));

                producer_q.push(
                    prio,
                    Msg {
                        producer: i as u8,
                        slot,
                    },
                );
            }
        }));
    }

    barrier.wait();
    let start = Instant::now();
    start_flag.store(true, AtomicOrdering::Release);

    for p in producers {
        p.join().unwrap();
    }
    consumer.join().unwrap();

    start.elapsed()
}

// ---------------------------
// Tests
// ---------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(producer: u8, slot: u16) -> Msg {
        Msg { producer, slot }
    }

    fn dual_priority_contract<Q: DualPrioQueue + Default>() {
        let q = Q::default();
        assert!(q.pop().is_none(), "empty queue must pop None");

        // Interleave classes; expect all HIGH first, FIFO within each class.
        q.push(LOW, msg(0, 1));
        q.push(HIGH, msg(0, 2));
        q.push(LOW, msg(0, 3));
        q.push(HIGH, msg(0, 4));

        let order: Vec<u16> = std::iter::from_fn(|| q.pop()).map(|m| m.slot).collect();
        assert_eq!(order, vec![2, 4, 1, 3]);
        assert!(q.pop().is_none());
    }

    #[test]
    fn mutex_heap_priority_and_fifo() {
        dual_priority_contract::<MutexBinaryHeapQueue>();
    }

    #[test]
    fn dual_seg_priority_and_fifo() {
        dual_priority_contract::<DualSegQueue>();
    }

    fn prio101_contract<Q: Prio101Queue + Default>() {
        let q = Q::default();
        assert!(q.pop().is_none(), "empty queue must pop None");

        // Push a spread of levels out of order, two per level to check FIFO.
        for (i, prio) in [0u8, 100, 37, 100, 0, 37].iter().enumerate() {
            q.push(*prio, msg(*prio, i as u16));
        }

        let order: Vec<(u8, u16)> = std::iter::from_fn(|| q.pop())
            .map(|m| (m.producer, m.slot))
            .collect();
        // Highest priority first; within a priority, insertion (FIFO) order.
        assert_eq!(
            order,
            vec![(100, 1), (100, 3), (37, 2), (37, 5), (0, 0), (0, 4)]
        );
        assert!(q.pop().is_none());
    }

    #[test]
    fn skiplist_priority_and_fifo() {
        prio101_contract::<SkiplistQueue>();
    }

    #[test]
    fn bitmap_priority_and_fifo() {
        prio101_contract::<BitmapQueue>();
    }

    fn concurrent_no_loss<Q: DualPrioQueue + Default>() {
        const PER_PRODUCER: u64 = 2_000;
        let q = Arc::new(Q::default());
        let mut handles = Vec::new();
        for p in 0..8u8 {
            let q = Arc::clone(&q);
            handles.push(thread::spawn(move || {
                for i in 0..PER_PRODUCER {
                    let prio = if i % 3 == 0 { HIGH } else { LOW };
                    q.push(prio, msg(p, (i % u16::MAX as u64) as u16));
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        let mut per_producer = [0u64; 8];
        while let Some(m) = q.pop() {
            per_producer[m.producer as usize] += 1;
        }
        assert_eq!(
            per_producer, [PER_PRODUCER; 8],
            "no message lost or duplicated"
        );
    }

    #[test]
    fn mutex_heap_concurrent_no_loss() {
        concurrent_no_loss::<MutexBinaryHeapQueue>();
    }

    #[test]
    fn dual_seg_concurrent_no_loss() {
        concurrent_no_loss::<DualSegQueue>();
    }

    #[test]
    fn producer_pool_slot_lifecycle() {
        let pool = ProducerPool::new(4, 128);
        let mut hint = 0usize;

        // All four slots can be acquired and are distinct.
        let mut got: Vec<u16> = (0..4).map(|_| pool.acquire_slot(&mut hint)).collect();
        got.sort_unstable();
        assert_eq!(got, vec![0, 1, 2, 3]);

        // Releasing makes a slot acquirable again.
        pool.release_slot(2);
        assert_eq!(pool.acquire_slot(&mut hint), 2);
    }

    #[test]
    fn producer_pool_payload_roundtrip() {
        let size = 200usize;
        let pool = ProducerPool::new(2, size);
        let mut hint = 0usize;
        let slot = pool.acquire_slot(&mut hint);

        let pattern = 0xA5u8;
        pool.write_payload(slot, pattern);

        // touch_payload XORs one byte per 64-byte line plus the last byte; all
        // bytes hold `pattern`, so the result is `pattern` for an odd number of
        // touches and 0 for an even number.
        let touches = size.div_ceil(64) + 1;
        let expected = if touches % 2 == 1 { pattern as u64 } else { 0 };
        assert_eq!(pool.touch_payload(slot), expected);
    }
}
