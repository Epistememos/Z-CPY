#include "zcpy/ingester.hpp"
#include "zcpy_bridge/lib.h"


namespace zcpy {
    Ingester::Ingester(const std::string& filename, std::size_t capacity, const uint32_t stream_id) : table_(filename, capacity), stream_id_(stream_id), ingested_count_(table_.size()) {

    }
    
    bool  Ingester::emplace(uint64_t timestamp_ns, double value) {
        return table_.emplace(timestamp_ns, value);
    }

    std::size_t Ingester::ingest() {
        std::lock_guard<std::mutex> lock(ingest_mutex_);
        const auto view = table_.committed_view();
        const rust::Slice<const zcpy::TelemetryPacket> slice{view.data() + ingested_count_, view.size() - ingested_count_};
       
        const std::size_t accepted = zcpy::ingest_packets(stream_id_, slice);
        if (accepted > 0) {
            ingested_count_ = view.size();
        }
        return accepted;
    }

}