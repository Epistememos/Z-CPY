#include "zcpy/memtable.hpp"
#include "zcpy_bridge/lib.h"
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <benchmark/benchmark.h>
#include <chrono>
#include <vector>
#include <algorithm>

static void BM_Ingest(benchmark::State& state) {
    
    constexpr std::size_t kTableCapacity = 100000000;
    std::uint64_t counter = 1;

    zcpy::MemTable table{kTableCapacity};
    
    // Saving timing per iteration for sub-us p99
    std::vector<std::int64_t> latencies_ns;
    latencies_ns.reserve(5'000'000);

    for (auto _ : state) {
        auto t_0 = std::chrono::steady_clock::now();
        if (!table.emplace(counter * 1'000ULL,
            static_cast<double>(counter) * 0.1
        )) {
          state.SkipWithError("Filled memtable.bin");  
        } 
        auto t_1 = std::chrono::steady_clock::now();
        latencies_ns.push_back((t_1 - t_0).count());
        counter++;
    }
    std::sort(latencies_ns.begin(), latencies_ns.end());
    const auto p99_value = latencies_ns[static_cast<size_t>(latencies_ns.size() * 0.99)];

    state.counters["p99_ns"] = static_cast<double>(p99_value);
}
BENCHMARK(BM_Ingest)->Iterations(5'000'000);
BENCHMARK_MAIN();