#include "docstore/document.h"
#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include <benchmark/benchmark.h>
#include <nlohmann/json.hpp>
#include <random>
#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <filesystem>
#include <gtest/gtest.h>

namespace docstore {

class DocumentStoreBenchmark {
public:
    DocumentStoreBenchmark() {
        leveldb::Status s;
        store_ = std::make_unique<DocumentStore>("/tmp/docstore_bench", s);
        EXPECT_TRUE(s.ok());

        // Create test collection with secondary key
        leveldb::Options options;
        options.primary_key = "id";
        options.secondary_key = "age";  // Add secondary key
        nlohmann::json schema = R"(
            {
                "fields": [
                    {"id": "integer"},
                    {"name": "string"},
                    {"age": "integer"},
                    {"email": "string"}
                ],
                "required": ["id", "name", "age"]
            }
        )"_json;

        s = store_->CreateCollection("users", options, schema);
        EXPECT_TRUE(s.ok());

        // Pre-populate with test data
        for (int i = 0; i < 10000; i++) {
            nlohmann::json doc;
            doc["id"] = i;
            doc["name"] = "user_" + std::to_string(i);
            doc["age"] = 20 + (i % 50);  // Ages from 20 to 69
            doc["email"] = "user_" + std::to_string(i) + "@example.com";
            store_->Insert("users", doc);
        }
    }

    ~DocumentStoreBenchmark() {
        if (std::filesystem::exists("/tmp/docstore_bench")) {
            std::filesystem::remove_all("/tmp/docstore_bench");
        }
    }

    std::unique_ptr<DocumentStore> store_;
};

// Benchmark for single document insert
static void BM_Insert(benchmark::State& state) {
    DocumentStoreBenchmark bench;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(10000, 20000);

    for (auto _ : state) {
        nlohmann::json doc;
        int id = dis(gen);
        doc["id"] = id;
        doc["name"] = "user_" + std::to_string(id);
        doc["age"] = 20 + (id % 50);
        doc["email"] = "user_" + std::to_string(id) + "@example.com";
        
        leveldb::Status s = bench.store_->Insert("users", doc);
        if (!s.ok()) {
            state.SkipWithError("Insert failed");
        }
    }
}
BENCHMARK(BM_Insert);

// Benchmark for single document get
static void BM_Get(benchmark::State& state) {
    DocumentStoreBenchmark bench;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 9999);

    for (auto _ : state) {
        std::string value;
        int id = dis(gen);
        leveldb::Status s = bench.store_->Get("users", std::to_string(id), value);
        if (!s.ok()) {
            state.SkipWithError("Get failed");
        }
    }
}
BENCHMARK(BM_Get);

// Benchmark for batch insert
static void BM_BatchInsert(benchmark::State& state) {
    DocumentStoreBenchmark bench;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(10000, 20000);

    for (auto _ : state) {
        std::vector<nlohmann::json> docs;
        for (int i = 0; i < state.range(0); i++) {
            nlohmann::json doc;
            int id = dis(gen);
            doc["id"] = id;
            doc["name"] = "user_" + std::to_string(id);
            doc["age"] = 20 + (id % 50);
            doc["email"] = "user_" + std::to_string(id) + "@example.com";
            docs.push_back(doc);
        }

        for (auto& doc : docs) {
            leveldb::Status s = bench.store_->Insert("users", doc);
            if (!s.ok()) {
                state.SkipWithError("Batch insert failed");
                break;
            }
        }
    }
}
BENCHMARK(BM_BatchInsert)->Range(10, 1000);

// Benchmark for concurrent operations
static void BM_ConcurrentOperations(benchmark::State& state) {
    DocumentStoreBenchmark bench;
    std::atomic<int> success_count{0};
    std::atomic<int> fail_count{0};

    for (auto _ : state) {
        std::vector<std::thread> threads;
        for (int i = 0; i < state.range(0); i++) {
            threads.emplace_back([&bench, i, &success_count, &fail_count]() {
                // 50% reads, 50% writes
                if (i % 2 == 0) {
                    // Write operation
                    nlohmann::json doc;
                    doc["id"] = 20000 + i;
                    doc["name"] = "user_" + std::to_string(20000 + i);
                    doc["age"] = 20 + (i % 50);
                    doc["email"] = "user_" + std::to_string(20000 + i) + "@example.com";
                    
                    leveldb::Status s = bench.store_->Insert("users", doc);
                    if (s.ok()) success_count++;
                    else fail_count++;
                } else {
                    // Read operation
                    std::string value;
                    int id = i % 10000;
                    leveldb::Status s = bench.store_->Get("users", std::to_string(id), value);
                    if (s.ok()) success_count++;
                    else fail_count++;
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }
    }
}
BENCHMARK(BM_ConcurrentOperations)->Range(2, 32);

// Benchmark for secondary key retrieval
static void BM_GetBySecondaryKey(benchmark::State& state) {
    DocumentStoreBenchmark bench;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(20, 69);  // Match the age range

    for (auto _ : state) {
        std::vector<leveldb::SecondayKeyReturnVal> results;
        int age = dis(gen);
        leveldb::Status s = bench.store_->GetSec("users", std::to_string(age), &results, 1000);
        if (!s.ok()) {
            state.SkipWithError("Secondary key retrieval failed");
        }
    }
}
BENCHMARK(BM_GetBySecondaryKey);

// Benchmark for unoptimized secondary key lookup using GetAll
static void BM_GetBySecondaryKeyUsingGetAll(benchmark::State& state) {
    DocumentStoreBenchmark bench;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(20, 69);

    for (auto _ : state) {
        std::vector<nlohmann::json> results;
        int target_age = dis(gen);
        
        // Get all documents first
        std::vector<nlohmann::json> all_docs;
        leveldb::Status s = bench.store_->GetAll("users", all_docs);
        if (!s.ok()) {
            state.SkipWithError("GetAll failed");
            continue;
        }
        
        // Filter by age
        for (const auto& doc : all_docs) {
            if (doc["age"] == target_age) {
                results.push_back(doc);
            }
        }
        
        // Prevent compiler from optimizing away the results
        benchmark::DoNotOptimize(results);
    }
}
BENCHMARK(BM_GetBySecondaryKeyUsingGetAll);

// Benchmark for range query on secondary key
static void BM_RangeGetBySecondaryKey(benchmark::State& state) {
    DocumentStoreBenchmark bench;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(20, 60);  // Leave room for range

    for (auto _ : state) {
        std::vector<leveldb::SecondayKeyReturnVal> results;
        int start_age = dis(gen);
        int end_age = start_age + 10;  // Query range of 10 years
        leveldb::Status s = bench.store_->RangeGetSec("users", 
            std::to_string(start_age), 
            std::to_string(end_age), 
            &results, 
            1000);
        if (!s.ok()) {
            state.SkipWithError("Secondary key range retrieval failed");
        }
    }
}
BENCHMARK(BM_RangeGetBySecondaryKey);

// Benchmark for range query using GetAll
static void BM_RangeGetUsingGetAll(benchmark::State& state) {
    DocumentStoreBenchmark bench;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(20, 60);  // Leave room for range

    for (auto _ : state) {
        std::vector<nlohmann::json> results;
        int start_age = dis(gen);
        int end_age = start_age + 10;  // Query range of 10 years
        
        // Get all documents first
        std::vector<nlohmann::json> all_docs;
        leveldb::Status s = bench.store_->GetAll("users", all_docs);
        if (!s.ok()) {
            state.SkipWithError("GetAll failed");
            continue;
        }
        
        // Filter by age range
        for (const auto& doc : all_docs) {
            int age = doc["age"];
            if (age >= start_age && age <= end_age) {
                results.push_back(doc);
            }
        }
        
        // Prevent compiler from optimizing away the results
        benchmark::DoNotOptimize(results);
    }
}
BENCHMARK(BM_RangeGetUsingGetAll);

static void BM_GetAll(benchmark::State& state) {
    DocumentStoreBenchmark bench;
    
    for (auto _ : state) {
        std::vector<nlohmann::json> documents;
        leveldb::Status s = bench.store_->GetAll("users", documents);
        if (!s.ok()) {
            state.SkipWithError("GetAll failed");
        }
        // Prevent compiler from optimizing away the results
        benchmark::DoNotOptimize(documents);
    }
}
BENCHMARK(BM_GetAll);
} // namespace docstore

BENCHMARK_MAIN(); 