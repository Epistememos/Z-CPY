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

    zcpy::MemTable table{"bench_ingest.bin", kTableCapacity};

    // Saving timing per iteration for sub-us p99
    std::vector<std::int64_t> latencies_ns;
    latencies_ns.reserve(5'000'000);

    for (auto _ : state) {
        // Start timer
        // N.B. Using steady_clock adds overhead. Performance jumped from 2.70 ns on avg per emplace to 29.1 ns on avg
        auto t_0 = std::chrono::steady_clock::now();
        if (!table.emplace(counter * 1'000ULL,
            static_cast<double>(counter) * 0.1
        )) {
          state.SkipWithError("Filled memtable.bin");  
        } 
        // End timer
        auto t_1 = std::chrono::steady_clock::now();
        latencies_ns.push_back((t_1 - t_0).count());
        counter++;
    }
    std::sort(latencies_ns.begin(), latencies_ns.end());
    const auto p99_value = latencies_ns[static_cast<size_t>(latencies_ns.size() * 0.99)];
    state.counters["p99_ns"] = static_cast<double>(p99_value);
}

static void BM_IngestFull(benchmark::State& state) {
    constexpr std::size_t kTableCapacity = 100000000;
    std::uint64_t counter = 1;

    zcpy::MemTable table{"bench_ingest_full.bin", kTableCapacity};

    std::vector<std::int64_t> latencies_ns;
    latencies_ns.reserve(10'000);

    for (auto _ : state) {
        auto t_0 = std::chrono::steady_clock::now();
        table.emplace(counter * 1'000ULL, static_cast<double>(counter) * 0.1);

        // Ingest one packet at a time
        const rust::Slice<const zcpy::TelemetryPacket> one{table.data() + (counter - 1), 1};
        zcpy::ingest_packets(0, one);

        auto t_1 = std::chrono::steady_clock::now();
        latencies_ns.push_back((t_1 - t_0).count());
        counter++;
    }
    std::sort(latencies_ns.begin(), latencies_ns.end());
    const auto p99_value = latencies_ns[static_cast<size_t>(latencies_ns.size() * 0.99)];
    state.counters["p99_ns"] = static_cast<double>(p99_value);

}

void BM_Query(benchmark::State& state) {

    constexpr std::size_t kTableCapacity = 100000000;
    std::uint64_t counter = 1;

    zcpy::MemTable table{"bench_query.bin", kTableCapacity};

    // Saving timing per iteration for sub-us p99
    std::vector<std::int64_t> latencies_ns;
    latencies_ns.reserve(5'000'000);

    for (int entries = 1; entries<5'000'000; ++entries) {
    if (!table.emplace(entries * 1'000ULL,
            static_cast<double>(entries) * 0.1
        )) {
          state.SkipWithError("Filled memtable.bin");  
        } 
    }

    for (auto _ : state) {
        auto t_0 = std::chrono::steady_clock::now();
        // query is a no discard ffucntion
        auto result = table.query(counter * 1'000ULL, (counter + 1) * 1'000ULL);
        benchmark::DoNotOptimize(result);
        
        auto t_1 = std::chrono::steady_clock::now();
        latencies_ns.push_back((t_1 - t_0).count());
        counter++;
    }
    std::sort(latencies_ns.begin(), latencies_ns.end());
    const auto p99_value = latencies_ns[static_cast<size_t>(latencies_ns.size() * 0.99)];
    state.counters["p99_ns"] = static_cast<double>(p99_value);

}

static void BM_IngestBatch(benchmark::State& state) {
    constexpr std::size_t kBatchSize = 100;
    constexpr std::size_t kTableCapacity = 100000000;
    std::uint64_t counter = 1;

    zcpy::MemTable table{"bench_ingest_bench.bin", kTableCapacity};

    std::vector<std::int64_t> latencies_ns;
    latencies_ns.reserve(10'000);

    for (auto _ : state) {
        auto t_0 = std::chrono::steady_clock::now();
        
        for (std::size_t i = 0; i < kBatchSize; ++i) {
            table.emplace(counter * 1'000ULL, static_cast<double>(counter) * 0.1);
            counter++;
        }
        // Ingest one packet at a time
        const rust::Slice<const zcpy::TelemetryPacket> batch{table.data() + (counter - 1 - kBatchSize), kBatchSize};
        zcpy::ingest_packets(4, batch);

        auto t_1 = std::chrono::steady_clock::now();
        latencies_ns.push_back((t_1 - t_0).count() / static_cast<std::int64_t>(kBatchSize));
    }
    std::sort(latencies_ns.begin(), latencies_ns.end());
    const auto p99_value = latencies_ns[static_cast<size_t>(latencies_ns.size() * 0.99)];
    state.counters["p99_ns"] = static_cast<double>(p99_value);

}

BENCHMARK(BM_Ingest)->Iterations(5'000'000);
BENCHMARK(BM_IngestFull)->Iterations(10'000);
BENCHMARK(BM_Query)->Iterations(5'000'000);
BENCHMARK(BM_IngestBatch)->Iterations(10'000);
BENCHMARK_MAIN();