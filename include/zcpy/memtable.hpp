#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <span>

// Forward-declare the cxx shared struct to decouple this header from the
// generated "src/lib.rs.h". The full definition is pulled in by the .cpp TU.
namespace zcpy {
struct TelemetryPacket;
} // namespace zcpy

namespace zcpy {

inline constexpr std::size_t kCacheLineBytes  = 64;
inline constexpr std::size_t kDefaultCapacity = 4'096;

/// Cache-line aligned write buffer for incoming telemetry.
///
/// Owns a contiguous, aligned slab allocated via std::aligned_alloc.
/// The slab pointer is stable for the object's lifetime, making it safe to
/// hand directly to Rust as a rust::Slice without any intervening copy.
///
/// Intended to be the hot-path staging layer ahead of an mmap flush region.
class alignas(kCacheLineBytes) MemTable {
public:
    explicit MemTable(std::size_t capacity = kDefaultCapacity);
    ~MemTable();

    MemTable(const MemTable&)            = delete;
    MemTable& operator=(const MemTable&) = delete;
    MemTable(MemTable&&)                 = delete;
    MemTable& operator=(MemTable&&)      = delete;

    [[nodiscard]] TelemetryPacket*       data()     noexcept;
    [[nodiscard]] const TelemetryPacket* data()     const noexcept;
    [[nodiscard]] std::size_t            capacity() const noexcept { return capacity_; }
    [[nodiscard]] std::size_t            size()     const noexcept;

    /// Appends a record at the next available slot. Lock-free, single-producer.
    /// Returns false if the buffer is at capacity (no partial write occurs).
    bool emplace(std::uint64_t timestamp_ns, double value) noexcept;
    void flush() noexcept;

    /// Non-owning view over all committed records.
    /// The pointer identity is preserved — the same address will be visible
    /// inside Rust after the FFI call, proving zero-copy transfer.
    [[nodiscard]] std::span<const TelemetryPacket> committed_view() const noexcept;

private:
    TelemetryPacket*         storage_;
    std::size_t              capacity_;
    std::atomic<std::size_t> size_{0};
};

} // namespace zcpy
