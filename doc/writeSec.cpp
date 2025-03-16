#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include <cassert>
#include <iostream>
#include <leveldb/db.h>
#include <leveldb/filter_policy.h>
#include <rapidjson/document.h>
#include <sstream>
#include <stdlib.h>

using namespace std;

int main() {
  leveldb::DB *db;
  leveldb::Options options;
  options.filter_policy = leveldb::NewBloomFilterPolicy(10);
  options.PrimaryAtt = "id";
  options.secondaryAtt = "age";
  options.create_if_missing = true;
  leveldb::Status status =
      leveldb::DB::Open(options, "/opt/test_level_db_idx1", &db);
  assert(status.ok());
  leveldb::ReadOptions roptions;
  leveldb::WriteOptions woptions;

  std::cout << "hello yall" << std::endl;

  // Add 10,000 key-value pairs
  for (int i = 0; i < 10000; ++i) {
    std::stringstream ss;
    ss << "{\n \"id\": " << i << ",\n \"age\": " << (i % 50 + 10)
       << ",\n \"name\": \"User" << i << "\"\n}";
    std::string json_string = ss.str();
    leveldb::Status put_status = db->Put(woptions, json_string);
    if (!put_status.ok()) {
      std::cerr << "Error putting key " << i << ": " << put_status.ToString()
                << std::endl;
    }
  }

  /*
        Using Secondary Index
   */

  vector<leveldb::SKeyReturnVal> values;
  leveldb::Status val =
      db->Get(roptions, leveldb::Slice(std::to_string(30)), &values, 10000);

  for (auto val : values) {
    std::cout << "Key: " << val.key << "\t";
    std::cout << "Value: " << val.value << "\n";
  }
  std::cout << "Found " << values.size() << " records with age 30."
            << std::endl;
  std::cout << "------------------------------------------------\n";

  /*
    Range Query
  */

  vector<leveldb::SKeyReturnVal> range_values;
  db->RangeLookUp(roptions, leveldb::Slice(std::to_string(30)),
                  leveldb::Slice(std::to_string(35)), &range_values, 10000);

  for (auto val : range_values) {
    std::cout << "Key: " << val.key << "\t";
    std::cout << "Value: " << val.value << "\n";
  }
  std::cout << "Found " << values.size()
            << " records with age between 30 and 35." << std::endl;
  std::cout << "------------------------------------------------\n";
  /*
    How this would without secondary index
  */
  rapidjson::Document doc;
  leveldb::Iterator *it = db->NewIterator(roptions);
  int count = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    leveldb::Slice key = it->key();
    leveldb::Slice value = it->value();
    std::string json_string = value.ToString();

    // Parse JSON to extract age
    rapidjson::Document doc;
    doc.Parse<0>(json_string.c_str());

    if (doc.HasParseError()) {
      std::cerr << "Error parsing JSON: " << doc.GetParseError() << '\n';
      continue; // Skip to the next record
    }

    if (doc.HasMember("age") && doc["age"].IsInt()) {
      int age = doc["age"].GetInt();
      if (age == 30) {
        std::cout << "Key: " << key.ToString() << ", Value: " << json_string
                  << std::endl;
        count++;
      }
    }
  }
  assert(it->status().ok()); // Check for any errors found during the scan
  std::cout << "Found " << count << " records with age 30." << std::endl;

  delete db;
  delete options.filter_policy;
  return 0;
}