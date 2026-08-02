#pragma once

#include <string>
#include <cstdint>
#include <cstddef>
#include "zcpy/memtable.hpp"

namespace zcpy {
    class Ingester {
    public:
        Ingester(const std::string& filename, std::size_t capacity, const uint32_t stream_id);
        bool emplace(uint64_t timestamp_ns, double value);
        std::size_t ingest();  
    private:
        zcpy::MemTable table_;
        uint32_t stream_id_;
        std::size_t ingested_count_;
    };
}