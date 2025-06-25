#ifndef STORAGE_DOCSTORE_INCLUDE_H
#define STORAGE_DOCSTORE_INCLUDE_H

#include "leveldb/options.h"
#include "leveldb/status.h"
#include "memory"
#include "nlohmann/json.hpp"
#include <leveldb/db.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <shared_mutex>
#include <map>
#include <mutex>
#include <iostream>

namespace docstore {

struct CollectionHandle {
  std::unique_ptr<leveldb::DB> db_;
  nlohmann::json metadata_;

  bool ApplySchemaCheck() { return this->metadata_.contains("schema"); }
};

class DocumentStore {
public:
  DocumentStore(const std::string &base_path, leveldb::Status &status);
  ~DocumentStore();

  // Disable copying
  // DocumentStore(const DocumentStore &) = delete;
  // DocumentStore &operator=(const DocumentStore &) = delete;

  leveldb::Status CreateCollection(const std::string &collection_name,
                                   leveldb::Options options,
                                   nlohmann::json &schema);
  leveldb::Status DropCollection(const std::string &collection_name);

  leveldb::Status Insert(const std::string &collection_name,
                         nlohmann::json &document);
  leveldb::Status Insert(const std::string &collection_name, std::string key,
                         std::string value);
  bool Update(const std::string &collection_name, const std::string &id,
              const nlohmann::json &document);
  bool Delete(const std::string &collection_name, const std::string &id);
  leveldb::Status Get(const std::string &collection_name, std::string key,
                      std::string &value);
  leveldb::Status GetSec(const std::string &collection_name,
                         const std::string &secondary_key,
                         std::vector<leveldb::SecondayKeyReturnVal> *value,
                         int top_k = 1000);
  leveldb::Status RangeGetSec(const std::string &collection_name,
                              const std::string &secondary_start_key,
                              const std::string &secondary_end_key,
                              std::vector<leveldb::SecondayKeyReturnVal> *value,
                              int top_k = 1000);
  leveldb::Status ExtendMetadata(const nlohmann::json &document,
                                 const nlohmann::json &schema,
                                 nlohmann::json &result);

  // Utility Methods
  bool isValidJSON(const nlohmann::json &document);
  leveldb::DB* GetOrCreateDB(const std::string& collection_name, leveldb::Status& s);

  // Metadata Helper functions
  leveldb::Status CheckCollectionInRegistry(const string &collection_name,
                                            nlohmann::json &metadata);
  leveldb::Status AddCollectionToRegistry(const string &collection_name,
                                          nlohmann::json &metadata);
  leveldb::Status OpenCollection(const std::string &collection_name,
                                 const nlohmann::json &metadata);
  leveldb::Status ValidateSchema(const nlohmann::json &document,
                                 const nlohmann::json &schema);

  // Get all documents from a collection
  leveldb::Status GetAll(const std::string &collection_name,
                        std::vector<nlohmann::json> &documents);
  leveldb::Status GetRange(const std::string &collection_name, const std::string& min_key, const std::string& max_key,
                        std::vector<nlohmann::json> &documents);

private:
  std::string base_path_;
  std::unique_ptr<leveldb::DB> collection_registry_;
  std::unordered_map<std::string, std::unique_ptr<leveldb::DB>> collections_;
  std::unordered_map<std::string, const leveldb::FilterPolicy*> filter_policies_;
  std::mutex collections_mutex_;
  leveldb::Options options_;

  bool validate_type(nlohmann::basic_json<> &document_value, std::string &type);
};
} // namespace docstore

#endif //  STORAGE_DOCSTORE_INCLUDE_H