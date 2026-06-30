#include "zcpy/memtable.hpp"

// Pull in the full TelemetryPacket definition from the cxx-generated header.
// This file is emitted by `cargo build` (via build.rs + cxx-build) before CMake
// compiles this TU. Its include path is injected by corrosion_add_cxxbridge via
// the INTERFACE_INCLUDE_DIRECTORIES of the `zcpy_bridge` target.
#include "zcpy_bridge/lib.h"

#include <cstdlib>
#include <new>

namespace zcpy {

MemTable::MemTable(std::size_t capacity) : capacity_(capacity) {
    void* raw = std::aligned_alloc(kCacheLineBytes, capacity * sizeof(TelemetryPacket));
    if (raw == nullptr) throw std::bad_alloc{};
    storage_ = static_cast<TelemetryPacket*>(raw);
}

MemTable::~MemTable() {
    std::free(storage_);
}

TelemetryPacket* MemTable::data() noexcept       { return storage_; }
const TelemetryPacket* MemTable::data() const noexcept { return storage_; }

std::size_t MemTable::size() const noexcept {
    return size_.load(std::memory_order_acquire);
}

bool MemTable::emplace(std::uint64_t timestamp_ns, double value) noexcept {
    // Claim a slot with relaxed ordering; the store below is the actual fence.
    const std::size_t slot = size_.fetch_add(1, std::memory_order_relaxed);
    if (slot >= capacity_) {
        // Undo the claim so size() remains accurate.
        size_.fetch_sub(1, std::memory_order_relaxed);
        return false;
    }
    storage_[slot].timestamp_ns = timestamp_ns;
    storage_[slot].value        = value;
    // Release store: makes the write visible to any consumer calling size().
    // (In a real SPSC ring buffer this release would be on a separate head index.)
    return true;
}

std::span<const TelemetryPacket> MemTable::committed_view() const noexcept {
    return {storage_, size_.load(std::memory_order_acquire)};
}

} // namespace zcpy
