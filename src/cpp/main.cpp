#include "zcpy/memtable.hpp"

// cxx-generated header: defines TelemetryPacket (in namespace zcpy) and
// declares `zcpy::ingest_packets(rust::Slice<const TelemetryPacket>) -> size_t`.
// Also transitively includes <rust/cxx.h> for the rust::Slice template.
#include "zcpy_bridge/lib.h"

#include <cstdint>
#include <cstdio>
#include <cstdlib>

int main() {
    constexpr std::size_t kBatchSize = 8;
    constexpr std::size_t kTableCapacity = 64;

    // Allocation lives entirely on the C++ side.
    zcpy::MemTable table{"memtable.bin", kTableCapacity};
    std::printf("[C++] Recovered %zu packets\n", table.size());

    // Synthetic batch generation.
    const std::uint64_t base = 1'000'000'000ULL + table.size() * 1'000ULL;
    if (table.size() > 0) {
        zcpy::seed_last_ts(table.data()[table.size() - 1].timestamp_ns);
    }

    if (!zcpy::wal_startup_check()) {
        std::fputs("[C++] WAL repair failed\n", stderr);
        return EXIT_FAILURE;
    }

    // WAL replay.
    const std::size_t replay_count = zcpy::wal_replay_len(table.size());
    for (std::size_t i = 0; i < replay_count; ++i) {
        const auto pkt = zcpy::wal_replay_packet(i);
        table.emplace(pkt.timestamp_ns, pkt.value);
    }
    if (replay_count > 0) {
        std::printf("[C++] Replayed %zu packets from WAL\n", replay_count);
    }

    // Capture the current size for the zero-copy FFI call.
    const std::size_t recovered = table.size();

    // Populate the MemTable with a batch of synthetic telemetry.
    for (std::uint64_t i = 0; i < kBatchSize; ++i) {
        const bool ok = table.emplace(
            base + i * 1'000ULL,          // t0 + i * 1 microsecond
            static_cast<double>(i) * 0.1  // synthetic signal ramp
        );
        if (!ok) {
            std::fputs("[C++] MemTable overflow — raise kBatchSize\n", stderr);
            return EXIT_FAILURE;
        }
    }

    const auto view     = table.committed_view();   // non-owning span
    const auto* cpp_ptr = view.data();              // stable address under test

    std::printf("[C++] MemTable buffer  @ %p  (%zu packets, %zu bytes)\n",
                static_cast<const void*>(cpp_ptr),
                view.size(),
                view.size() * sizeof(zcpy::TelemetryPacket));

    // Zero-copy FFI call. The TelemetryPacket slab is not touched by any
    // allocator on the Rust side.
    const rust::Slice<const zcpy::TelemetryPacket> rs_slice{view.data() + recovered, view.size() - recovered};

    const std::size_t ingested = zcpy::ingest_packets(rs_slice);

    // Rust prints its slice pointer to stderr above; it must match cpp_ptr.
    std::printf("[C++] ingest_packets   → %zu packets accepted\n", ingested);
    std::printf("[C++] Zero-copy proof  : C++ ptr %p == Rust-visible ptr (see stderr)\n",
                static_cast<const void*>(cpp_ptr));

    // Validation gate tests.
    // 1. Internally out-of-order: second timestamp goes backwards.
    const zcpy::TelemetryPacket out_of_order[] = {
        {.timestamp_ns = 2'000'000'000ULL, .value = 1.0},
        {.timestamp_ns = 1'500'000'000ULL, .value = 2.0},
    };
    const std::size_t r1 = zcpy::ingest_packets({out_of_order, 2});
    std::printf("[C++] out-of-order batch → %zu accepted (expect 0)\n", r1);

    // 2. Internally ordered, but starts before the last accepted timestamp
    //    (good batch ended at t₀ + 7 µs) — exercises the cross-batch gate.
    const zcpy::TelemetryPacket stale[] = {
        {.timestamp_ns = 1'000'003'000ULL, .value = 3.0},
        {.timestamp_ns = 1'000'004'000ULL, .value = 4.0},
    };
    const std::size_t r2 = zcpy::ingest_packets({stale, 2});
    std::printf("[C++] stale batch        → %zu accepted (expect 0)\n", r2);

    if (r1 != 0 || r2 != 0) {
        std::fputs("[C++] FAIL: validation gate accepted a bad batch\n", stderr);
        return EXIT_FAILURE;
    }

    const auto results = table.query(base, base + 3'000ULL);
    std::printf("[C++] query [base, base+3µs] → %zu packets\n", results.size());

    auto amd_table = std::make_unique<zcpy::MemTable>("amd.bin", 64);
    auto nvda_table = std::make_unique<zcpy::MemTable>("nvda.bin", 128);

    for (std::uint64_t i = 0; i < 64; ++i) {
        const bool amd = amd_table->emplace(
            base + i * 1'000ULL,          // t0 + i * 1 microsecond
            static_cast<double>(i) * 0.1  // synthetic signal ramp
        );
        const bool nvda = nvda_table->emplace(
            base + i * 1'000ULL,          // t0 + i * 1 microsecond
            static_cast<double>(i) * 0.1  // synthetic signal ramp
        );
        if (!amd || !nvda) {
            std::fputs("[C++] MemTable overflow — raise kBatchSize\n", stderr);
            return EXIT_FAILURE;
        }
    }

    const auto amd_view     = amd_table->committed_view();   // non-owning span
    const auto* amd_ptr = amd_view.data();              // stable address under test
    std::printf("[C++] AMD MemTable buffer  @ %p  (%zu packets, %zu bytes)\n",
                static_cast<const void*>(amd_ptr),
                amd_view.size(),
                amd_view.size() * sizeof(zcpy::TelemetryPacket));

      
    const auto nvda_view     = nvda_table->committed_view();   // non-owning span
    const auto* nvda_ptr = nvda_view.data();              // stable address under test
    std::printf("[C++] NVDA MemTable buffer  @ %p  (%zu packets, %zu bytes)\n",
                static_cast<const void*>(nvda_ptr),
                nvda_view.size(),
                nvda_view.size() * sizeof(zcpy::TelemetryPacket));
    
    

    return EXIT_SUCCESS;
}
