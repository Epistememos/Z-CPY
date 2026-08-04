#include "zcpy/queue.hpp"
#include "zcpy_bridge/lib.h"

namespace zcpy {

LockFreeQueue::LockFreeQueue(std::size_t capacity)
    : capacity_(capacity), head_(0), storage_(capacity) {}

bool LockFreeQueue::push(TelemetryPacket packet) {
    const std::size_t slot = head_.fetch_add(1, std::memory_order_relaxed);
    if (slot >= capacity_) {
        head_.fetch_sub(1, std::memory_order_relaxed);
        return false;
    }
    storage_[slot] = packet;
    return true;
}

}