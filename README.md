# Z-CPY

**A hybrid C++/Rust embeddable time-series storage engine built on one rule: data is written once and never copied again.**

Telemetry packets land in a memory-mapped, cache-aligned buffer on the C++ side, are validated in place by a Rust ingestion layer through a zero-copy FFI boundary, and are persisted by the kernel from those same pages — one copy of the data, end to end, from ingest to disk.

```
                 ┌──────────────────── one shared buffer ───────────────────┐
  C++ writer ──▶ │  mmap'd slab (cache-aligned, file-backed: memtable.bin)  │ ──▶ disk
                 └──────────────────────────────────────────────────────────┘
                        ▲ pointer only (rust::Slice — 16 bytes)
                        │
                 Rust ingestion gate: monotonicity validation, in place
```

## Why

Time-series ingestion (market data, sensor telemetry, metrics) is dominated by two costs that conventional pipelines pay on every packet: **memory-bandwidth burned on copies** (network buffer → app buffer → language boundary → write queue → file: the same bytes moved 4–5×) and **allocator jitter** on the hot path. Z-CPY's architecture eliminates both: the ingest path performs zero copies and zero allocations per batch. At demo scale this is invisible; the point is an architecture that survives millions of packets/second without redesign.

## Architecture

| Layer | Language | Responsibility |
|---|---|---|
| **MemTable** | C++20 | Cache-line-aligned (64 B) slab, `mmap`-backed (`MAP_SHARED`) — writes land in the kernel page cache and *are* the file. Atomic slot claims (single-producer), startup recovery scan, `msync` flush control. |
| **Ingestion gate** | Rust 2021 | Validates batches in place through a borrowed slice — strictly monotonic timestamps, within and across batches (all-or-nothing accept). Cross-batch high-water mark (`AtomicU64`), seeded from recovered data on startup. |
| **FFI bridge** | [cxx](https://cxx.rs) | `TelemetryPacket` (16 B, trivially copyable) shared by layout between both languages. Batches cross as `rust::Slice` — a pointer + length, never a copy. Verified at runtime: both sides print the same buffer address. |
| **Build** | CMake + [Corrosion](https://github.com/corrosion-rs/corrosion) | Single `cmake --build` drives Cargo, generates the cxx bridge, and links the Rust staticlib into the C++ executable. |

### Durability model

Writes are durable via the mmap'd file: `emplace` dirties page-cache pages; the kernel flushes them lazily, `flush()` requests it explicitly (`MS_ASYNC`), and the destructor guarantees it (`MS_SYNC`). On startup, the committed packet count is recovered by scanning for the first zero timestamp (sound because `ftruncate` zero-fills and no valid packet has timestamp 0), and the validation gate is re-seeded from the recovered tail — a restarted engine rejects batches that would overlap data already on disk.

Writes are also appended to `wal.bin` before acknowledgement — same raw bytes, sequential append, cheapest possible disk write. A crash after the WAL write but before the kernel flushes the mmap is recoverable; a torn tail (`file_size % 16 != 0`) is detectable. Crash replay is next on the roadmap.

### Deliberate design decisions

- **Fixed-size pre-allocated file** — no remapping on the hot path (`mremap` is Linux-only anyway); overflow is an explicit, tested failure.
- **The memory image is the file format** — no serialization, no parsing on recovery. Trades schema evolution and cross-architecture portability for a zero-cost write path; the same trade kdb+, LMDB, and Arrow IPC make.
- **Single-producer by contract** — slot claims and the validation gate assume one ingesting thread; documented rather than locked. The known TOCTOU (`load → validate → store` on the gate) is impossible under the contract; `compare_exchange` is the escape hatch if it ever changes.
- **All-or-nothing batch acceptance** — a batch with any ordering violation is rejected whole, leaving no trace. Out-of-order telemetry signals an upstream fault; silently reordering would mask it.

## Build & run

Requires CMake ≥ 3.25, a C++20 compiler, and a Rust stable toolchain. Corrosion is fetched at configure time.

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build -j4
./build/zcpy_engine
```

The binary is self-testing: it ingests a batch, proves pointer identity across the FFI boundary, sends two deliberately invalid batches (internally out-of-order; stale versus the high-water mark), and exits non-zero if the gate accepts either. Run it twice — the second run recovers the first run's packets and appends after them:

```
[C++] Recovered 8 packets
[C++] MemTable buffer  @ 0x104b88000  (16 packets, 256 bytes)
[Rust] ingest_packets  @ 0x104b88000  (16 packets)      ← same address: zero-copy
[C++] ingest_packets   → 16 packets accepted
[Rust] ERROR: non-monotonic timestamp 1500000000 < 2000000000
[C++] out-of-order batch → 0 accepted (expect 0)
[C++] stale batch        → 0 accepted (expect 0)
```

`memtable.bin` is the storage file; inspect it with `xxd memtable.bin` (16 bytes per packet: little-endian `u64` nanosecond timestamp + IEEE 754 `f64` value). Delete it to reset.

## Repository layout

```
├── CMakeLists.txt          build orchestrator: Corrosion + cxxbridge + C++ exe
├── Cargo.toml / build.rs   Rust staticlib crate; cxx bridge generation
├── include/zcpy/
│   └── memtable.hpp        MemTable interface (cache-aligned, single-producer)
└── src/
    ├── lib.rs              #[cxx::bridge]: shared struct + FFI surface; gate state
    ├── ingestion.rs        batch validation logic (pure, testable)
    └── cpp/
        ├── memtable.cpp    mmap lifecycle, recovery scan, emplace, flush
        └── main.cpp        demo + self-tests (exit 1 on gate regression)
```

## Roadmap

- [x] Hybrid build: CMake + Corrosion + cxx, zero-copy FFI proof
- [x] mmap-backed MemTable with startup recovery and explicit flush
- [x] Monotonic-timestamp ingestion gate, persistent across restarts
- [x] **Write-ahead log (append path)** — append-before-acknowledge; `wal.bin` stores raw packet bytes, identical layout to `memtable.bin`; WAL failure blocks acknowledgement
- [ ] WAL crash replay — torn-tail detection (`file_size % 16`), replay into memtable on startup
- [ ] High-water-mark flush signal (background compaction trigger)
- [ ] Read path: binary-searched time-range scans over the mmap (zero-copy reads)
- [ ] Benchmarks: ingest throughput + p99 latency (criterion); allocation-free hot path validation
- [ ] Stateful `Ingester` handle over the bridge (multi-stream support)

## Progress log

Development history with rationale: [PROGRESS.md](PROGRESS.md)
