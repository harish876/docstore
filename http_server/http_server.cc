#include <crow.h>
#include <iostream>
#include <string>
#include <vector>

#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

#include "rapidjson/document.h"
#include "unordered_set"

int main(int argc, char *argv[]) {
  // Process command line arguments
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <database_path> [port]" << std::endl;
    return 1;
  }

  std::string db_path = argv[1];
  int port = (argc > 2) ? std::stoi(argv[2]) : 8080;

  // Create two DB instances
  leveldb::DB *db_with_bloom;
  leveldb::DB *db_without_bloom;

  // Configure options for DB with Bloom filter
  leveldb::Options options_with_bloom;
  options_with_bloom.filter_policy = leveldb::NewBloomFilterPolicy(20);
  options_with_bloom.create_if_missing = true;
  options_with_bloom.primary_key = "id";
  options_with_bloom.secondary_key = "age";

  // Configure options for DB without Bloom filter
  leveldb::Options options_without_bloom;
  options_without_bloom.filter_policy = nullptr; // No Bloom filter
  options_without_bloom.create_if_missing = true;
  options_without_bloom.primary_key = "id";

  // Open both databases
  leveldb::Status status_with_bloom = leveldb::DB::Open(
      options_with_bloom, db_path + "with_bloom", &db_with_bloom);
  if (!status_with_bloom.ok()) {
    std::cerr << "Unable to open/create database with Bloom filter: "
              << status_with_bloom.ToString() << std::endl;
    return 1;
  }

  leveldb::Status status_without_bloom = leveldb::DB::Open(
      options_without_bloom, db_path + "without_bloom", &db_without_bloom);
  if (!status_without_bloom.ok()) {
    std::cerr << "Unable to open/create database without Bloom filter: "
              << status_without_bloom.ToString() << std::endl;
    delete db_with_bloom;
    return 1;
  }

  crow::SimpleApp app;

  // Primary key get - specify which DB to use with ?bloom=true/false parameter
  CROW_ROUTE(app, "/db/get/<string>")
      .methods("GET"_method)([db_with_bloom,
                              db_without_bloom](const crow::request &req,
                                                std::string primary_key) {
        // Determine which DB to use
        auto use_bloom = req.url_params.get("bloom");
        leveldb::DB *db_to_use =
            (use_bloom && std::string(use_bloom) == "false") ? db_without_bloom
                                                             : db_with_bloom;

        // Get value by primary key
        std::string value;
        leveldb::Status s =
            db_to_use->Get(leveldb::ReadOptions(), primary_key, &value);

        if (s.ok()) {
          // Return the JSON value
          return crow::response(200, value);
        } else if (s.IsNotFound()) {
          return crow::response(404, "{\"error\": \"Key not found\"}");
        } else {
          return crow::response(500, "{\"error\": \"" + s.ToString() + "\"}");
        }
      });

  // Secondary key query - with parameter to select DB
  CROW_ROUTE(app, "/db/query")
      .methods("GET"_method)([db_with_bloom,
                              db_without_bloom](const crow::request &req) {
        // Get query parameters
        auto secondary_key = req.url_params.get("key");
        auto limit_param = req.url_params.get("limit");
        auto use_secondary = req.url_params.get("use_secondary");

        // Determine which DB to use

        leveldb::DB *db_to_use = nullptr;
        if (use_secondary) {
          std::cout << "Using Db with bloom " << std::endl;
          db_to_use = db_with_bloom;
        } else {
          std::cout << "Using Db without bloom " << std::endl;
          db_to_use = db_without_bloom;
        }

        int limit = 10;
        if (limit_param) {
          try {
            limit = std::stoi(limit_param);
          } catch (...) {
            return crow::response(400,
                                  "{\"error\": \"Invalid 'limit' parameter\"}");
          }
        }

        if (!use_secondary) {
          std::unique_ptr<leveldb::Iterator> it(
              db_to_use->NewIterator(leveldb::ReadOptions()));
          std::vector<std::string> results;
          int target_key_value = std::stoi(secondary_key);

          for (it->SeekToFirst(); it->Valid(); it->Next()) {
            std::string value = it->value().ToString();

            std::string sec_key_str;
            rapidjson::Document doc;
            if (doc.Parse<0>(value.c_str()).HasParseError() ||
                !doc.HasMember("age")) {
              continue;
            }
            if (doc.HasMember("age") && doc["age"].IsInt()) {
              int age = doc["age"].GetInt();
              if (age == target_key_value) {
                results.push_back(value);
              }
            }
          }

          if (!it->status().ok()) {
            return crow::response(500, "{\"error\": \"Iterator error: " +
                                           it->status().ToString() + "\"}");
          }

          std::string json_results = "[";
          for (size_t i = 0; i < results.size() && i < limit; i++) {
            if (i > 0)
              json_results += ",";
            json_results += results[i];
          }
          json_results += "]";

          if (results.empty()) {
            return crow::response(200, "application/json", "[]");
          } else {
            return crow::response(200, "application/json", json_results);
          }
        }

        if (!secondary_key) {
          return crow::response(400,
                                "{\"error\": \"Missing 'key' parameter\"}");
        }

        std::vector<leveldb::SKeyReturnVal> results;
        leveldb::Status s =
            db_to_use->Get(leveldb::ReadOptions(),
                           leveldb::Slice(secondary_key), &results, limit);

        if (s.ok()) {
          std::string json_results = "[";
          for (size_t i = 0; i < results.size(); i++) {
            if (i > 0)
              json_results += ",";
            json_results += results[i].value;
          }
          json_results += "]";
          return crow::response(200, "application/json", json_results);
        } else if (s.IsNotFound()) {
          return crow::response(404, "{\"error\": \"No records found\"}");
        } else {
          return crow::response(500, "{\"error\": \"" + s.ToString() + "\"}");
        }
      });

  // Put to both DBs to keep them in sync
  CROW_ROUTE(app, "/db/put")
      .methods("POST"_method)([db_with_bloom,
                               db_without_bloom](const crow::request &req) {
        auto body = req.body;
        if (body.empty()) {
          return crow::response(400, "{\"error\": \"Empty request body\"}");
        }

        // Write to both databases
        leveldb::Status s1 = db_with_bloom->Put(leveldb::WriteOptions(), body);
        leveldb::Status s2 =
            db_without_bloom->Put(leveldb::WriteOptions(), body);

        if (s1.ok() && s2.ok()) {
          return crow::response(200, "{\"status\": \"success\"}");
        } else {
          std::string error = "";
          if (!s1.ok())
            error += "With Bloom: " + s1.ToString();
          if (!s2.ok()) {
            if (!error.empty())
              error += ", ";
            error += "Without Bloom: " + s2.ToString();
          }
          return crow::response(500, "{\"error\": \"" + error + "\"}");
        }
      });

  // Add a new endpoint for stats/comparison
  CROW_ROUTE(app, "/db/stats")
      .methods("GET"_method)(
          [db_with_bloom, db_without_bloom](const crow::request &req) {
            // This would be expanded in a real implementation
            // to return performance statistics, etc.
            crow::json::wvalue stats;
            stats["bloom_filter_enabled"]["status"] = "active";
            stats["no_bloom_filter"]["status"] = "active";
            return crow::response(200, stats);
          });

  // Bulk insertion endpoint
  CROW_ROUTE(app, "/db/bulk-insert")
      .methods("POST"_method)([db_with_bloom,
                               db_without_bloom](const crow::request &req) {
        // Parse parameters from JSON body
        rapidjson::Document doc;
        if (doc.Parse<0>(req.body.c_str()).HasParseError()) {
          return crow::response(400, "{\"error\": \"Invalid JSON body\"}");
        }

        // Get number of records to insert
        if (!doc.HasMember("numRecords") || !doc["numRecords"].IsInt()) {
          return crow::response(
              400,
              "{\"error\": \"Missing or invalid 'numRecords' parameter\"}");
        }
        int numRecords = doc["numRecords"].GetInt();

        // Get which DB to use (both, bloom only, or no-bloom only)
        bool useBloom = true;
        bool useNoBloom = true;
        if (doc.HasMember("useBloom") && doc["useBloom"].IsBool()) {
          useBloom = doc["useBloom"].GetBool();
        }
        if (doc.HasMember("useNoBloom") && doc["useNoBloom"].IsBool()) {
          useNoBloom = doc["useNoBloom"].GetBool();
        }

        // Set up timing
        auto startTime = std::chrono::high_resolution_clock::now();
        int successCount = 0;
        int errorCount = 0;

        // Set up write options
        leveldb::WriteOptions woptions;

        // Insert data
        for (int i = 0; i < numRecords; ++i) {
          // Create sample JSON document
          std::stringstream ss;
          ss << "{\n \"id\": " << i << ",\n \"age\": " << (i % 50 + 10)
             << ",\n \"name\": \"User" << i << "\"\n}";
          std::string json_string = ss.str();

          // Insert into selected DBs
          bool success = true;
          if (useBloom) {
            leveldb::Status s = db_with_bloom->Put(woptions, json_string);
            if (!s.ok()) {
              errorCount++;
              success = false;
            }
          }

          if (useNoBloom) {
            leveldb::Status s = db_without_bloom->Put(woptions, json_string);
            if (!s.ok()) {
              errorCount++;
              success = false;
            }
          }

          if (success) {
            successCount++;
          }
        }

        // Calculate time taken
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            endTime - startTime);

        // Build response
        crow::json::wvalue result;
        result["success"] = true;
        result["recordsRequested"] = numRecords;
        result["recordsInserted"] = successCount;
        result["recordsFailed"] = errorCount;
        result["timeMs"] = duration.count();
        result["usedBloomFilter"] = useBloom;
        result["usedNoBloomFilter"] = useNoBloom;

        return crow::response(200, result);
      });

  // Also add an endpoint to run performance comparison tests
  CROW_ROUTE(app, "/db/performance-test")
      .methods("POST"_method)([db_with_bloom,
                               db_without_bloom](const crow::request &req) {
        // Parse parameters
        rapidjson::Document doc;
        if (doc.Parse<0>(req.body.c_str()).HasParseError()) {
          return crow::response(400, "{\"error\": \"Invalid JSON body\"}");
        }

        // Get target age to search for
        int targetAge = 30; // default
        if (doc.HasMember("targetAge") && doc["targetAge"].IsInt()) {
          targetAge = doc["targetAge"].GetInt();
        }

        // Results object
        crow::json::wvalue results;

        // Test with bloom filter
        {
          auto startWithBloom = std::chrono::high_resolution_clock::now();

          std::vector<leveldb::SKeyReturnVal> values;
          leveldb::ReadOptions roptions;
          db_with_bloom->Get(roptions,
                             leveldb::Slice(std::to_string(targetAge)), &values,
                             1000);

          auto endWithBloom = std::chrono::high_resolution_clock::now();
          auto bloomDuration =
              std::chrono::duration_cast<std::chrono::microseconds>(
                  endWithBloom - startWithBloom);

          results["withBloomFilter"]["timeUs"] = bloomDuration.count();
          results["withBloomFilter"]["recordsFound"] = values.size();
        }

        // Test without bloom filter
        {
          auto startNoBloom = std::chrono::high_resolution_clock::now();

          std::vector<leveldb::SKeyReturnVal> values;
          leveldb::ReadOptions roptions;
          db_without_bloom->Get(roptions,
                                leveldb::Slice(std::to_string(targetAge)),
                                &values, 1000);

          auto endNoBloom = std::chrono::high_resolution_clock::now();
          auto noBloomDuration =
              std::chrono::duration_cast<std::chrono::microseconds>(
                  endNoBloom - startNoBloom);

          results["withoutBloomFilter"]["timeUs"] = noBloomDuration.count();
          results["withoutBloomFilter"]["recordsFound"] = values.size();
        }

        // Full scan test on bloom filter DB (for comparison)
        {
          auto startScan = std::chrono::high_resolution_clock::now();

          leveldb::ReadOptions roptions;
          leveldb::Iterator *it = db_with_bloom->NewIterator(roptions);
          int count = 0;

          for (it->SeekToFirst(); it->Valid(); it->Next()) {
            rapidjson::Document doc;
            doc.Parse<0>(it->value().ToString().c_str());
            if (!doc.HasParseError() && doc.HasMember("age") &&
                doc["age"].IsInt()) {
              if (doc["age"].GetInt() == targetAge) {
                count++;
              }
            }
          }

          auto endScan = std::chrono::high_resolution_clock::now();
          auto scanDuration =
              std::chrono::duration_cast<std::chrono::microseconds>(endScan -
                                                                    startScan);

          results["fullScan"]["timeUs"] = scanDuration.count();
          results["fullScan"]["recordsFound"] = count;

          delete it;
        }

        // Calculate speedups
        double bloomVsNoBloom =
            std::stod(results["withoutBloomFilter"]["timeUs"].dump()) /
            std::stod(results["withBloomFilter"]["timeUs"].dump());
        double bloomVsScan =
            std::stod(results["fullScan"]["timeUs"].dump()) /
            std::stod(results["withBloomFilter"]["timeUs"].dump());

        results["speedups"]["bloomVsNoBloom"] = bloomVsNoBloom;
        results["speedups"]["bloomVsFullScan"] = bloomVsScan;
        results["targetAge"] = targetAge;

        return crow::response(200, results);
      });

  // Add this route after your existing CROW_ROUTE definitions
  CROW_ROUTE(app, "/db/range")
      .methods("GET"_method)([db_with_bloom,
                              db_without_bloom](const crow::request &req) {
        // Get range query parameters
        auto start_value = req.url_params.get("start");
        auto end_value = req.url_params.get("end");
        auto limit_param = req.url_params.get("limit");

        // Validate required parameters
        if (!start_value || !end_value) {
          return crow::response(
              400, "{\"error\": \"Missing 'start' or 'end' parameters\"}");
        }

        // Determine which DB to use based on bloom parameter
        leveldb::DB *db_to_use = db_with_bloom;

        // Parse limit parameter (default: 10)
        int limit = 10;
        if (limit_param) {
          try {
            limit = std::stoi(limit_param);
          } catch (...) {
            return crow::response(400,
                                  "{\"error\": \"Invalid 'limit' parameter\"}");
          }
        }

        // Set up result containers
        std::vector<leveldb::SKeyReturnVal> results;
        std::unordered_set<std::string> result_set;

        // Track performance
        auto start_time = std::chrono::high_resolution_clock::now();

        // Execute range query
        leveldb::Status s = db_to_use->RangeGet(
            leveldb::ReadOptions(), std::string(start_value),
            std::string(end_value), &results, limit);

        auto end_time = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            end_time - start_time);

        // Build response based on query result
        if (s.ok()) {
          // Format results as JSON array
          std::string json_results = "[";
          for (size_t i = 0; i < results.size(); i++) {
            if (i > 0)
              json_results += ",";
            json_results += results[i].value;
          }
          json_results += "]";

          // Add performance metadata
          crow::json::wvalue response;
          response["results"] = crow::json::load(json_results);
          response["count"] = results.size();
          response["query_time_us"] = duration.count();
          response["bloom_filter"] = true;

          return crow::response(200, response);
        } else if (s.IsNotFound()) {
          return crow::response(404,
                                "{\"error\": \"No records found in range\"}");
        } else {
          return crow::response(500, "{\"error\": \"" + s.ToString() + "\"}");
        }
      });

  // Start the server
  std::cout << "Starting LevelDB HTTP server on port " << port << std::endl;
  std::cout << "Database paths:" << std::endl;
  std::cout << "  With Bloom filter: " << db_path + "_with_bloom" << std::endl;
  std::cout << "  Without Bloom filter: " << db_path + "_without_bloom"
            << std::endl;
  app.port(port).multithreaded().run();

  // Clean up both DB instances
  delete db_with_bloom;
  delete db_without_bloom;
  delete options_with_bloom
      .filter_policy; // Don't forget to free the Bloom filter

  return 0;
}