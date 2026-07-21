# Progress Log

Working sessions on Z-CPY, with what was built, what broke, and what was learned.
Newest first.

---

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
