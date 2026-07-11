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

    // ── Allocation lives entirely on the C++ side ─────────────────────────
    zcpy::MemTable table{kTableCapacity};
    std::printf("[C++] Recovered %zu packets\n", table.size());


    for (std::uint64_t i = 0; i < kBatchSize; ++i) {
        const bool ok = table.emplace(
            1'000'000'000ULL + i * 1'000ULL,  // t₀ + i·1 µs
            static_cast<double>(i) * 0.1      // synthetic signal ramp
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

    // ── Zero-copy FFI call ────────────────────────────────────────────────
    // rust::Slice<T> is { ptr: *const T, len: usize } — 16 bytes on the stack.
    // The TelemetryPacket slab is not touched by any allocator on the Rust side.
    const rust::Slice<const zcpy::TelemetryPacket> rs_slice{view.data(), view.size()};

    const std::size_t ingested = zcpy::ingest_packets(rs_slice);

    // ── Verification ─────────────────────────────────────────────────────
    // Rust prints its slice pointer to stderr above; it must match cpp_ptr.
    std::printf("[C++] ingest_packets   → %zu packets accepted\n", ingested);
    std::printf("[C++] Zero-copy proof  : C++ ptr %p == Rust-visible ptr (see stderr)\n",
                static_cast<const void*>(cpp_ptr));

    return EXIT_SUCCESS;
}
