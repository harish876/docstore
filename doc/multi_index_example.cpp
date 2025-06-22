#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include <cassert>
#include <iostream>
#include <leveldb/db.h>
#include <leveldb/filter_policy.h>
#include <sstream>
#include <unordered_set>
#include <cstdlib>

using namespace std;

int main(int argc, char* argv[]) {
  // Parse command line arguments
  int num_values = 100000; // default value
  if (argc > 1) {
    num_values = atoi(argv[1]);
    if (num_values <= 0) {
      std::cerr << "Error: Number of values must be positive. Using default value of 100000." << std::endl;
      num_values = 100000;
    }
  }
  
  std::cout << "Inserting " << num_values << " documents..." << std::endl;

  leveldb::DB *db;
  leveldb::Options options;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);
  options.primary_key = "id";
  
  // NEW: Configure multiple secondary indexes
  options.secondary_key = "age";
  options.secondary_keys = {"age", "city", "salary"};
  
  options.create_if_missing = true;
  leveldb::Status status =
      leveldb::DB::Open(options, "/tmp/multi_index_db", &db);
  if (!status.ok()) {
    std::cerr << "Failed to open database: " << status.ToString() << std::endl;
    delete options.filter_policy;
    return 1;
  }
  
  leveldb::ReadOptions roptions;
  leveldb::WriteOptions woptions;

  std::cout << "=== Multi-Index Document Store Demo ===" << std::endl;

  // Insert sample documents with multiple indexed attributes
  for (int i = 0; i < num_values; ++i) {
    std::stringstream ss;
    ss << "{\n"
       << " \"id\": " << i << ",\n"
       << " \"age\": " << (20 + (i % 50)) << ",\n"
       << " \"city\": \"City" << (i % 10) << "\",\n"
       << " \"salary\": " << (30000 + (i % 70000)) << ",\n"
       << " \"name\": \"User" << i << "\"\n"
       << "}";
    std::string json_string = ss.str();
    leveldb::Status put_status = db->Put(woptions, json_string);
    if (!put_status.ok()) {
      std::cerr << "Error putting document " << i << ": " << put_status.ToString()
                << std::endl;
    }
  }

  std::cout << "\n=== Query Examples ===" << std::endl;

  // Query by age (range index)
  std::cout << "\n1. Finding users aged 25-30:" << std::endl;
  vector<leveldb::SecondayKeyReturnVal> age_results;
  leveldb::Status age_status = db->RangeGetBySecondaryKey(
      roptions, "age", leveldb::Slice("25"), leveldb::Slice("31"), 
      &age_results, 10);
  
  if (age_status.ok()) {
    std::cout << "Found " << age_results.size() << " users aged 25-30:" << std::endl;
    for (const auto& result : age_results) {
      std::cout << "  ID: " << result.key << ", Value: " << result.value << std::endl;
    }
  }

  // Query by city (hash index)
  std::cout << "\n2. Finding users in City5:" << std::endl;
  vector<leveldb::SecondayKeyReturnVal> city_results;
  leveldb::Status city_status = db->GetBySecondaryKey(
      roptions, "city", leveldb::Slice("City5"), &city_results, 10);
  
  if (city_status.ok()) {
    std::cout << "Found " << city_results.size() << " users in City5:" << std::endl;
    for (const auto& result : city_results) {
      std::cout << "  ID: " << result.key << ", Value: " << result.value << std::endl;
    }
  }

  // Query by salary range
  std::cout << "\n3. Finding users with salary 50000-60000:" << std::endl;
  vector<leveldb::SecondayKeyReturnVal> salary_results;
  leveldb::Status salary_status = db->RangeGetBySecondaryKey(
      roptions, "salary", leveldb::Slice("50000"), leveldb::Slice("60001"), 
      &salary_results, 10);
  
  if (salary_status.ok()) {
    std::cout << "Found " << salary_results.size() << " users with salary 50k-60k:" << std::endl;
    for (const auto& result : salary_results) {
      std::cout << "  ID: " << result.key << ", Value: " << result.value << std::endl;
    }
  }

  // Complex query: Users in City3 with age 30-35
  std::cout << "\n4. Complex query: Users in City3 aged 30-35:" << std::endl;
  vector<leveldb::SecondayKeyReturnVal> city3_results, age30_results;
  
  // Get users in City3
  db->GetBySecondaryKey(roptions, "city", leveldb::Slice("City3"), &city3_results, 100);
  
  // Get users aged 30-35
  db->RangeGetBySecondaryKey(roptions, "age", leveldb::Slice("30"), leveldb::Slice("36"), 
                             &age30_results, 100);
  
  // Find intersection
  std::unordered_set<std::string> city3_ids, age30_ids;
  for (const auto& result : city3_results) city3_ids.insert(result.key);
  for (const auto& result : age30_results) age30_ids.insert(result.key);
  
  std::cout << "Users in both City3 and aged 30-35:" << std::endl;
  for (const auto& id : city3_ids) {
    if (age30_ids.find(id) != age30_ids.end()) {
      std::cout << "  ID: " << id << std::endl;
    }
  }

  delete db;
  delete options.filter_policy;
  return 0;
} 