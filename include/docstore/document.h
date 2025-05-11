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
  CollectionHandle *GetCollectionHandle(const std::string &collection_name,
                                        leveldb::Status &s);
  bool isValidJSON(const nlohmann::json &document);

  // Metadata Helper functions
  leveldb::Status CheckCollectionInRegistry(const string &collection_name,
                                            nlohmann::json &metadata);
  leveldb::Status AddCollectionToRegistry(const string &collection_name,
                                          nlohmann::json &metadata);
  leveldb::Status OpenCollection(const std::string &collection_name,
                                 nlohmann::json &metadata);
  leveldb::Status ValidateSchema(const nlohmann::json &document,
                                 const nlohmann::json &schema);

private:
  std::string base_path_;
  // TODO: change to an LRU cache
  std::unordered_map<std::string, CollectionHandle> collections_handle_;
  leveldb::Options options_;
  // This is a simple key value store which contains all collections
  std::unique_ptr<leveldb::DB> collection_registry_;
  bool validate_type(nlohmann::basic_json<> &document_value, std::string &type);
};
} // namespace docstore

#endif //  STORAGE_DOCSTORE_INCLUDE_H