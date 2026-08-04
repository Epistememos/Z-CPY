#pragma once

#include "zcpy/memtable.hpp"  // for TelemetryPacket forward declare, or include directly
#include <vector>


namespace zcpy {
    class LockFreeQueue {
        public:
            explicit LockFreeQueue(std::size_t capacity);
            bool push(TelemetryPacket packet);
        private:
            std::size_t              capacity_;
            std::atomic<std::size_t> head_;
            std::vector<TelemetryPacket> storage_;
    };
}