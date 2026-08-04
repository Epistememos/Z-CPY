# Progress Log

Working sessions on Z-CPY, with what was built, what broke, and what was learned.
Newest first.

---

## 2026-08-03 — LockFreeQueue scaffolded: producer-side push, no wraparound

**Built:** `zcpy::LockFreeQueue` (`include/zcpy/queue.hpp`, `src/cpp/queue.cpp`) — stage 1 of the single-writer-principle plan from last session. Fixed-capacity `std::vector<TelemetryPacket>` storage, an `std::atomic<std::size_t> head_` producers claim via `fetch_add` (same pattern as `MemTable::emplace`), bounds-checked with a rollback on overflow. No consumer/reader side yet, no wraparound — just proving the producer side (`push`) is a real, working implementation, not a stub.

**Theory studied to get the design right this time:** read through the LMAX Disruptor paper and Preshing's lock-free/memory-order posts. Worked through `MemTable::emplace`'s own comment ("In a real SPSC ring buffer this release would be on a separate head index") and identified a real, if currently latent, gap: `size_`'s `fetch_add(relaxed)` happens *before* the data write, with no subsequent `release` operation — so a reader's `acquire` load of `size_` has no formally-guaranteed pairing proving the data write is visible. Works today only under the single-producer contract; the fix (splitting "claim" from "publish" into two separate steps, matching Disruptor's claim-sequence/cursor split) is deferred to `LockFreeQueue`'s consumer side, which doesn't exist yet — no reader means this gap doesn't bite today.

**Bugs hit while scaffolding:** `std::vector<TelemetryPacket>` needs the *complete* type to allocate storage — `queue.hpp` only transitively sees `memtable.hpp`'s forward declaration of `TelemetryPacket`, causing "incomplete type" errors until `queue.cpp` added `#include "zcpy_bridge/lib.h"` (the cxx-generated header with the full definition) — same lesson as `Ingester`'s `MemTable` value-member requirement, now recurring in a new spot.

Next: stage 2 — the dedicated writer thread that drains the queue and becomes the *only* caller of `Ingester::emplace`/`ingest()`.

## 2026-08-02 — Found a real data race under concurrency, before it shipped

**Built:** a multi-threaded stress test in `main.cpp` — three `std::thread`s, each looping `emplace`/`ingest()` calls against the *same* `Ingester` concurrently. Added `-fsanitize=thread` support via a new CMake option, `ZCPY_ENABLE_TSAN` (default `OFF`, since ThreadSanitizer roughly doubles memory use and slows execution 5-15x — not something to leave on by default). Verified the option works from a completely clean `rm -rf build` reconfigure, not just a stale cached command-line flag.

**Bug found — and it was loud enough to see without even needing the sanitizer report:** `Ingester::ingest()`'s `ingested_count_` is a plain `std::size_t`, unsynchronized. Concurrent threads calling `ingest()` could all read the same stale count before any of them updated it, causing a lost update — the counter got stuck, and every subsequent call resubmitted the same already-processed slice forever, visible as thousands of identical repeated `non-monotonic timestamp` rejections in the output.

**Fixed:** added `ingest_mutex_` (`std::mutex`) to `Ingester` and wrapped `ingest()`'s entire read-check-submit-update sequence in a `std::lock_guard`, so only one thread can execute that critical section at a time. This resolves the specific lost-update race.

**Deeper problem identified, not yet fixed:** the mutex doesn't address a separate issue — `MemTable::emplace`'s atomic slot-claim (`size_.fetch_add`) guarantees memory safety (no two threads ever write the same slot) but says nothing about *timestamp order*. Concurrent producers could get slots assigned in an order that doesn't match their timestamps, silently breaking the sorted-array invariant `query()`'s binary search depends on — a correctness bug ThreadSanitizer can't catch, since no memory is unsafely shared.

**Direction chosen:** rather than patch this with a mutex (which would undercut the whole "lock-free multi-producer" goal), decided to build toward the single-writer principle — the pattern used in the LMAX Disruptor architecture. Multiple producer threads push into a lock-free MPSC queue (reusing the same atomic-fetch-add slot-claim pattern already proven safe in `MemTable::emplace`); a single dedicated writer thread drains it and performs the actual `emplace`/`ingest()` calls sequentially, guaranteeing order without ever blocking producers. Staged as: (1) build the queue's producer side only, bounded, no wraparound; (2) add the dedicated writer thread; (3) add ring-buffer wraparound for indefinite operation.

## 2026-08-01 — `Ingester`: bundling MemTable + stream_id into one object

**Built:** a new class, `zcpy::Ingester` (`include/zcpy/ingester.hpp`, `src/cpp/ingester.cpp`), owning a `MemTable` and a `const uint32_t stream_id_` together. Exposes `emplace(timestamp_ns, value)` and `ingest()` — the latter internally slices only the packets not yet submitted (tracked via a private `ingested_count_`, seeded from `table_.size()` at construction so recovered packets from a prior run aren't resubmitted) and calls `zcpy::ingest_packets(stream_id_, slice)`. Callers never type a raw `stream_id` again. `main.cpp`'s AMD/NVDA blocks were rewritten around it: `zcpy::Ingester amd{"amd.bin", 64, 1};` replaces the old `unique_ptr<MemTable>` + manual slice-building + manually-typed stream ID at every call site.

**Why this exists:** a session ago, `main.cpp` had a real bug — NVDA's batch was accidentally validated against AMD's `stream_id`. The compiler didn't catch it because nothing linked a `MemTable` to its `stream_id`; they were just two separately-typed values kept in sync by convention. `Ingester` makes that mismatch structurally impossible: there's only one object to reason about per stream, and its `ingest()` method always uses its own `stream_id_`.

**C++ mechanics reinforced along the way:** member initialization order always follows *declaration* order in the class, not the order written in the initializer list — this is why `ingested_count_` (needing `table_.size()`) must be declared *after* `table_` in the header. Also: `stream_id_` was made `const` since it's set once at construction and never mutated again — no atomic needed, since atomics only matter for values that are *written* concurrently, and this one never is after construction.

## 2026-07-29 — Group commit proven: batching amortizes the fsync floor ~100x

**Built:** `BM_IngestBatch` — ingests 100 packets per `ingest_packets` call instead of 1, dividing each measured latency by `kBatchSize` before storing it, so the resulting p99 reflects *effective per-packet cost* rather than per-call cost. No new WAL code was needed: `wal::append` already takes a `&[TelemetryPacket]` slice and does exactly one `write_all` + one `sync_all` for the whole slice, regardless of how many packets are in it — group commit was already the mechanism, just never benchmarked with a realistic batch size before today.

**Result:** effective per-packet p99 dropped from 6.04 ms (`BM_IngestFull`, 1 packet/call) to 60.5 µs (`BM_IngestBatch`, 100 packets/call) — a ~100x reduction. Mean per-packet cost dropped by roughly the same ratio (133 µs → ~1.3 µs, once `BM_IngestBatch`'s raw `CPU` figure is manually divided by 100). Confirms the fsync floor is a fixed per-call cost, not proportional to payload size — spreading it across more packets shrinks the effective per-packet cost almost linearly with batch size.

**Bugs caught along the way:** (1) Google Benchmark auto-calibrates by calling the benchmark function multiple times before settling on an iteration count — each call resets local variables like `counter`, but `LAST_TS_MAP` is a process-wide static that doesn't reset between those calls, causing spurious rejections. Fixed the same way as the other benchmarks: pin `->Iterations(N)` to skip calibration. (2) `BM_IngestBatch` initially reused `BM_IngestFull`'s file (`bench_ingest_full.bin`) — since all benchmarks run in one process, `MemTable`'s recovery scan picked up thousands of leftover packets, misaligning the slice-offset math (`counter`-based, assumed an empty table) against the actual slot indices `emplace` returned. Fixed by giving it its own dedicated file.

**Clarified along the way:** `Time`/`CPU` (Google Benchmark's own columns) always measure cost *per loop iteration*, with zero awareness of what's inside that iteration — they don't automatically divide by batch size. `p99_ns` is a fully custom metric: every value in `latencies_ns` is manually divided by `kBatchSize` at insertion time, before sorting and taking the percentile. Two independent measurement systems reported side by side in the same output row, not the same number processed twice.

## 2026-07-29 — Read-path benchmark: BM_Query closes the performance story

**Built:** `BM_Query` in `bench_ingest.cpp` — fills a table with 5,000,000 packets (timestamps starting at `1`, never `0`, since `0` is the recovery-scan sentinel for "empty slot"), then times `MemTable::query` doing a binary-search time-range lookup against that dataset. Result: 81.1 ns mean, 125 ns p99.

**Bugs caught while writing it:** (1) a malformed `for` loop header (`100000, 1` isn't a valid condition); (2) the fill loop never advanced its own counter, so every packet would have gotten the identical timestamp; (3) the query range wasn't scaled to match the stored timestamps (multiples of 1,000), so it would have searched an empty range every call; (4) starting the fill loop at `0` instead of `1` — would have collided with the zero-timestamp recovery sentinel; (5) `query()`'s return value was discarded, triggering a `[[nodiscard]]` warning and risking dead-code elimination — fixed with `benchmark::DoNotOptimize`.

**Result — the full performance picture is now three numbers:** write path (`emplace`, 28.7 ns mean / 42 ns p99), read path (`query`, 81.1 ns mean / 125 ns p99), and durable ingest (`emplace` + `ingest_packets` with WAL fsync, 225 µs mean / 6.17 ms p99). Write and read are both sub-microsecond, consistent with `O(log n)` binary search over 5M entries (~22 comparisons); only the fsync-durable path pays real disk I/O cost.

## 2026-07-29 — AMD/NVDA end-to-end proof: two live streams through the full stack

**Built:** `main.cpp`'s AMD/NVDA blocks now exercise the complete path, not just `MemTable`-level isolation: `emplace` 64 packets into each table, build a `rust::Slice` from each table's own `committed_view()`, and call `zcpy::ingest_packets` with distinct stream IDs (AMD = `1`, NVDA = `2`). Both streams validated and accepted all 64 packets independently — proving `LAST_TS_MAP` and the per-stream WAL genuinely don't interfere with each other, not just that they're structurally separate.

**Bugs caught while wiring this up:** (1) tried calling `ingest_packets` as a `MemTable` method (`amd_table->ingest_packets(...)`) — it's a free function from the Rust bridge, not something `MemTable` knows how to do; (2) passed the leftover adversarial-test array (`stale`) instead of each table's own freshly-emplaced data — would have validated data that was never actually written to either table; (3) used `->` instead of `.` on `std::span` values (`committed_view()` returns a value, not a pointer — only `unique_ptr<MemTable>` needs `->`); (4) passed AMD's stream ID (`1`) to NVDA's `ingest_packets` call — a silent correctness bug the compiler can't catch, exactly the fragility of manually keeping a `MemTable` and its `stream_id` in sync by convention rather than by structure.

**Design note:** that last bug is the concrete case for the `Ingester` handle already on the roadmap — bundling a `MemTable` and its `stream_id` into one object would make this class of mistake impossible instead of just avoidable.

## 2026-07-29 — Per-stream WAL: multi-stream support fully wired end-to-end

**Built:** `wal.rs`'s three functions (`append`, `torn_tail_detection`, `replay`) all gained a `stream_id: u32` parameter and now derive a per-stream filename (`format!("wal_{}.bin", stream_id)`) instead of the hardcoded `"wal.bin"`. The single cached `WAL_FILE: Mutex<Option<File>>` was replaced with `WAL_MAP: LazyLock<Mutex<HashMap<u32, File>>>` — one open file handle per stream, all behind one lock. `WAL_REPLAY` in `lib.rs` became `WAL_REPLAY_MAP: LazyLock<Mutex<HashMap<u32, Vec<TelemetryPacket>>>>` for the same reason — a single shared replay buffer would let one stream's `wal_replay_len` call silently overwrite another's before `wal_replay_packet` read it back.

**Bug caught before it shipped:** an early draft of `wal_replay_len` tried to `get_mut` a stream's entry from the map *before* ever inserting it — always `None`, always panics on `.unwrap()`. Not a compile error — a runtime-only bug, since `Option::unwrap()` on `None` is only checked when the code actually runs. Fixed by capturing the length from the freshly-computed `Vec` directly, then inserting it into the map, instead of inserting and immediately reading back out.

**Design note on locking:** `WAL_MAP` uses one lock for the whole map rather than a lock per stream. This means two streams' WAL writes serialize behind each other even though they're logically independent — a real bottleneck under concurrent multi-threaded ingestion, but a non-issue today under the documented single-producer contract (one thread, one stream at a time). Documented as a known future optimization (nested per-stream locks behind a fast outer lookup) rather than built now, since building it today would optimize for a workload that doesn't exist yet.

**Verified:** full run — recovery, WAL replay, ingest, both adversarial validation tests, range query, and both isolated AMD/NVDA streams — all pass with `stream_id` threaded through every layer (`MemTable`, `LAST_TS_MAP`, `WAL_MAP`, `WAL_REPLAY_MAP`).

## 2026-07-28 — Per-stream LAST_TS gate on the Rust side

**Built:** `ingest_packets` and `seed_last_ts` both gained a `stream_id: u32` parameter — chose an integer over a string key since streams can be many instances of "the same kind of thing" (no natural unique name) and hashing a `u32` is cheaper than hashing a string on a per-packet hot path. `LAST_TS` (a single global `AtomicU64`) was replaced with `LAST_TS_MAP: LazyLock<Mutex<HashMap<u32, u64>>>` — keyed by stream, one high-water mark per stream instead of one for the whole process. `LazyLock` was necessary rather than a plain `static` initializer because `HashMap::new()` isn't a `const fn` (its default hasher seeds itself with runtime randomness), unlike `Vec::new()`.

**Bug caught before it shipped:** an early draft read the map (`.lock()`) and then, later in the same function, locked it again while the first `MutexGuard` was still in scope — `std::sync::Mutex` isn't reentrant, so this would have deadlocked (hung forever, not panicked) the first time `ingest_packets` accepted a batch. Fixed by wrapping the read in its own nested block so the guard drops before the second lock.

**Verified:** rebuilt with all five C++ call sites (`main.cpp` ×4, `bench_ingest.cpp` ×1) updated to pass `stream_id = 0` for the existing single-stream flow; full run — recovery, WAL replay, ingest, both adversarial validation tests, range query — behaves identically to before.

**Known gap, not yet fixed:** the WAL (`wal.rs`) is still one global `wal.bin` file. AMD and NVDA now have separate `MemTable`s and separate `LAST_TS` gates, but would still share one WAL file if routed through `ingest_packets` today — that's the next step.

## 2026-07-27 — Multi-stream proof: per-stream MemTable isolation

**Built:** `MemTable`'s constructor now takes a filename (`const std::string&`) instead of hardcoding `"memtable.bin"` — `MemTable(const std::string& filename, std::size_t capacity = kDefaultCapacity)`. Since `MemTable` can't be copied or moved (deleted constructors), multiple instances are held via `std::unique_ptr`. `main.cpp` now constructs two independent streams (`amd.bin`, `nvda.bin`) alongside the original table, emplaces into each, and prints all three buffer addresses — confirmed three distinct addresses, three distinct files, no shared state.

**Design decision:** chose one `MemTable` per stream over a single shared buffer with a `stream_id` tag. A shared buffer would break the binary-search read path's core invariant (the whole buffer being one globally sorted array by timestamp) and require rebuilding per-stream separation by hand anyway. Per-stream tables get that isolation structurally, for free.

**Known gap, not yet fixed:** `LAST_TS`, `ingest_packets`, and the WAL are still global on the Rust side. AMD and NVDA are proven isolated at the `MemTable`/C++ level, but are not yet safe to route through `ingest_packets` — one stream's timestamps would incorrectly gate the other's. That's the next multi-stream step: making the validation gate and WAL per-stream too.

## 2026-07-26 — Fixed WAL file-reopen: p99 dropped 8x

**Built:** `wal::append` no longer reopens `wal.bin` on every call. A `static Mutex<Option<File>>` caches the file handle across calls — opened once on first use, reused afterward. Getting a usable `&mut File` out of the cached `Option` without moving it out of the `Mutex` guard: check `guard.is_none()`, open and store via `*guard = Some(file)` if empty, then `guard.as_mut().unwrap()` to borrow it in place.

**Result:** `BM_IngestFull` mean CPU dropped 251 µs → 128 µs (~2x); p99 dropped 48.0 ms → 6.07 ms (~8x). The p99 improvement is much larger than the mean improvement — the file-reopen was an intermittent, spiky cost (filesystem metadata churn on open/close), not a constant one, so removing it disproportionately helped worst-case latency.

**What's left as the real floor:** `fsync` itself. 128 µs mean is now roughly in line with typical single-fsync latency — that cost doesn't go away without batching multiple writes into one fsync call (group commit), which is a bigger architectural change, not a bug fix.

## 2026-07-25 — Full-path benchmark: fsync-durable ingest cost

**Built:** `BM_IngestFull` — times `emplace` + `ingest_packets` together (FFI call, Rust validation, WAL append with fsync). Passes a length-1 slice of just the newest packet per call (`table.data() + (counter - 1)`), avoiding the same re-validation-of-everything bug the `recovered` slice fix in `main.cpp` already caught — passing the growing `committed_view()` here would re-validate the whole WAL history on every call instead of just the new packet.

**Result:** 251 µs mean / 48.0 ms p99 — roughly a million-times slower than the write-only path (29.3 ns / 42 ns), entirely attributable to `fsync`. Ruled out the `eprintln!` debug logging in `ingest_packets` as a factor (removing it barely moved the numbers) — confirmed the cost is real I/O, not print overhead.

**Root cause of the fat p99 tail:** `wal::append` reopens `wal.bin` from scratch on every call (`OpenOptions::new().open(...)` inside the function) before writing and fsyncing — full open/close syscall cost stacked on top of the fsync itself, every packet. Not fixed yet.

**Context vs production systems:** most real WAL-based engines (RocksDB, Kafka) don't fsync per write — they batch many writes into one fsync (group commit) or skip local fsync and rely on replication instead. Per-write fsync, which this benchmark measures, is the deliberately slow/safe end of the spectrum.

Next: keep the WAL file handle open across calls instead of reopening every time; longer-term, batch fsyncs across multiple writes (group commit).

## 2026-07-24 — First real benchmark: write-path p99

**Built:** `bench/bench_ingest.cpp` — `BM_Ingest` times `MemTable::emplace` in isolation (no FFI, no WAL). Fixed at 5,000,000 iterations via `->Iterations(...)` instead of Google Benchmark's default auto-scaling, after discovering emplace is cheap enough that auto-scaling could exceed any reasonable pre-allocated table capacity. Manually timed each call with `steady_clock::now()` around it, collected into a sorted vector, and reported p99 via `state.counters`.

**Result:** mean 2.70 ns per `emplace` (Release build, no manual timing overhead). With manual per-call timing overhead included (needed to compute p99), mean rises to 29.1 ns and p99 lands at 42 ns — still sub-microsecond.

**Bugs hit:** stale `memtable.bin` from a prior failed run ate into table capacity before the loop even started (recovery scan on construction) — fixed by deleting it between runs. Also needed `CMAKE_BUILD_TYPE=Release`; Debug build timings are unreliable (Google Benchmark warns about this directly).

**Note on precision:** average latency, not p99, is what Google Benchmark's built-in timer reports. Getting p99 required manually timing every call and computing the percentile ourselves — the built-in aggregate timer can't expose that.

Next: benchmark the full path (`emplace` + `ingest_packets`, including WAL fsync) for comparison.

## 2026-07-22 — Google Benchmark wired into CMake

**Built:** `zcpy_bench` target added via `FetchContent` (Google Benchmark), same pattern as Corrosion. Links `zcpy_bridge`/`zcpy` so it can time the full FFI path, not just Rust in isolation (criterion can't cross the bridge). `-O2 -march=native` set explicitly.

**Next problem:** `LAST_TS` will reject repeated timestamps across benchmark iterations — need fresh monotonic timestamps per loop. `bench/bench_ingest.cpp` not written yet.

## 2026-07-21 — Read path: binary search time-range query

**Built:** `MemTable::query(uint64_t start_ns, uint64_t end_ns)` in `memtable.cpp`. Uses `std::lower_bound` to find the first packet with `timestamp_ns >= start_ns` and `std::upper_bound` to find the first packet past `end_ns`. Returns `std::span<const TelemetryPacket>` — a non-owning slice directly into the mmap'd slab. Zero copy: no data moved, just two pointers into existing memory.

**Bug hit:** `upper_bound` comparator had operands reversed (`p.timestamp_ns < ts` instead of `ts < p.timestamp_ns`), causing `higher < lower` and a `size_t` underflow → 18446744073709551560 packets returned. Fixed by restoring the correct comparator. Rule: comparator body is always `first_param < second_param`; only the parameter order differs between `lower_bound` and `upper_bound`.

**Verified:** `query(base, base + 3000)` returns exactly 4 packets on a fresh run.

Next: benchmarks (ingest throughput + p99 latency), then multi-stream support.

## 2026-07-20 — WAL crash replay + fsync + torn tail detection

**Built:** Three remaining WAL pieces.

`fsync` — `file.sync_all()` added to `append` after `write_all`. Blocks until the OS confirms bytes hit physical storage. Without it, `write_all` only reaches the OS buffer; a power cut before the OS flushes would silently lose the WAL entry.

`torn_tail_detection` — new function in `wal.rs`. Opens `wal.bin` with write access, checks `file_size % 16`. If not a multiple of 16, a partial packet was written before a crash; `set_len` truncates back to the last clean 16-byte boundary. Missing file returns `true` — nothing to repair.

Crash replay — `wal_replay_len(memtable_count)` and `wal_replay_packet(index)` exposed across the cxx bridge. `wal_replay_len` calls `wal::replay`, caches the result in a static `Mutex<Vec<TelemetryPacket>>`, and returns the count of missing packets. `wal_replay_packet` returns one packet by index. `main.cpp` loops over the count and emplaces each packet back into the memtable before the normal write loop. Verified: deleting `memtable.bin` and restarting correctly replays all 48 WAL packets before writing 8 new ones.

**Why the static Vec:** `Vec<TelemetryPacket>` can't cross the cxx bridge directly — cxx only allows simple types. Caching on the Rust side and fetching one packet at a time keeps the bridge clean.

Next: read path — binary search over the mmap'd slab for time-range queries.

## 2026-07-18 — Write-ahead log (append path)

**Built:** `src/wal.rs` — new Rust module with `pub fn append(packets: &[TelemetryPacket]) -> bool`. Opens `wal.bin` with `O_APPEND | O_CREAT`, reinterprets the packet slice as raw bytes via an unsafe `slice::from_raw_parts` cast, and writes them in one `write_all` call. Wired into `ingest_packets` in `lib.rs`: WAL is written after validation passes and before `LAST_TS` is updated — if the WAL write fails, the function returns 0 (no acknowledgement without durability). Verified: after two runs `wal.bin` is 256 bytes and its first 256 bytes are identical to `memtable.bin`.

**Bug fixed along the way:** `main.cpp` was passing `committed_view()` — all packets including recovered ones — to `ingest_packets`. On run 2, `LAST_TS` was seeded to the last recovered timestamp, so packet 0 of the full view (an old packet with a lower timestamp) failed validation immediately. Fixed by capturing `table.size()` before the write loop and slicing the view to only the newly emplaced packets (`view.data() + recovered`, `view.size() - recovered`).

**Design note:** `wal.bin` stores the same raw 16-byte struct layout as `memtable.bin` — no serialization, same zero-cost write path. Sequential appends are the cheapest disk operation; the WAL never rewrites old bytes so a crash can't corrupt prior records. Torn tail is detectable: `file_size % 16 != 0`.

Next: add `wal.bin` to `.gitignore`, then torn-tail detection and crash replay.

## 2026-07-18 — Gate persistence across restarts

**Built:** `seed_last_ts(u64)` — first new FFI function since the scaffold. After the
recovery scan, C++ passes the last recovered packet's timestamp across the bridge so
the Rust gate's high-water mark (`LAST_TS`) starts where the previous run ended,
instead of at 0.

**Why it was needed:** `LAST_TS` is process state; it died with the process while
`memtable.bin` didn't — the same durable-data/amnesiac-counter bug as the Day-2
recovery scan, one layer up. Unseeded, a restarted engine would accept batches that
overlap data already on disk.

**Design note:** the recovered tail lives on the C++ side, the gate on the Rust side —
someone has to carry the value across at startup. A one-line bridge function beats
persisting Rust-side state separately.

Next: write-ahead log (append path first, then fsync contract, then replay).

## 2026-07-13 — Ingestion validation gate (Rust)

**Built:** monotonic-timestamp validation in `process_batch` — a single pass that
checks each timestamp is strictly greater than the previous, seeded with the
cross-batch high-water mark so within-batch and cross-batch checks collapse into one
loop. All-or-nothing: any violation rejects the whole batch and leaves no state
behind. `LAST_TS: AtomicU64` added in lib.rs (single-producer by contract; the
`load → validate → store` TOCTOU is acknowledged and documented rather than locked).
main.cpp gained two adversarial self-tests — an internally out-of-order batch and a
stale batch — with a non-zero exit code if either is accepted.

**What broke — the best bug so far:** the new validator immediately rejected the
*good* batch. Root cause: main.cpp hardcoded the same timestamps every run, so after
two persisted runs the file contained the same time range twice — genuinely
non-monotonic data. The gate was right; the writer was wrong. Fixed by deriving each
run's timestamp base from the recovered count, so runs append monotonically.

**Second lesson:** the fix initially "passed" only because `memtable.bin` had been
deleted between runs — the test was validating fresh state, not the fix. Caught by
re-reading the diff and re-running with a prediction ("run 2 must fail"). Tests only
prove something when you know why they pass.

## 2026-07-11 — Startup recovery + explicit flush

**Built:** recovery scan in the MemTable constructor — walk the mapped slab until the
first zero timestamp and store that index into `size_`. Sound because `ftruncate`
guarantees zero-fill and no valid packet has timestamp 0. Also `flush()` with
`msync(MS_ASYNC)` (request persistence without blocking the write path; the
destructor's `MS_SYNC` remains the guaranteed flush), and main.cpp decoupled table
capacity (64) from batch size (8).

**The bug it fixed:** data persisted across runs but the committed count didn't —
a restarted process saw valid packets and a `size_` of 0, and silently overwrote
slot 0 onward. Invisible in testing because reruns wrote identical bytes.

**Known limitation, accepted for v0.1:** the zero-timestamp sentinel assumes writes
commit in order and timestamps are never 0. Real engines use an explicit header or
WAL instead — which is where this is headed.

## 2026-07-06 — mmap-backed persistence

**Built:** replaced `aligned_alloc` with the mmap lifecycle in the MemTable:
`open(O_RDWR|O_CREAT)` → `ftruncate` to capacity → `mmap(PROT_READ|PROT_WRITE,
MAP_SHARED)` → `close(fd)`; destructor does `msync(MS_SYNC)` → `munmap`. Every error
path checked (a failed `ftruncate` otherwise surfaces later as SIGBUS on first
write — the failure far from its cause). Verified with `xxd`: packet bytes present
in `memtable.bin` after process exit.

**The point:** writes into the slab now *are* writes into the file — the kernel page
cache is the write buffer and there is no serialize step. The memory image is the
file format: zero-copy extended from the FFI boundary down to disk. Accepted trade:
the file is tied to this architecture's endianness and struct layout (the kdb+/LMDB
trade).

## 2026-06-29 — Scaffold: hybrid build + zero-copy FFI proof

**Built:** the full skeleton. CMake + Corrosion (FetchContent) drives Cargo and links
the Rust staticlib into the C++ executable; cxx bridge defines the shared 16-byte
`TelemetryPacket` and passes batches as `rust::Slice` (pointer + length). MemTable
v0: cache-line-aligned `aligned_alloc` slab with atomic single-producer slot claims.
main.cpp proves the zero-copy claim at runtime — C++ and Rust print the same buffer
address.

**Build gotchas worth recording:** `corrosion_add_cxxbridge` prepends `src/` itself
(pass `FILES lib.rs`, not `src/lib.rs`) and emits the generated header as
`zcpy_bridge/lib.h`, not `src/lib.rs.h`.
