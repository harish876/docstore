#ifndef STORAGE_DOCSTORE_INCLUDE_H
#define STORAGE_DOCSTORE_INCLUDE_H

#include "leveldb/options.h"
#include "leveldb/status.h"
#include "memory"
#include <leveldb/db.h>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <vector>

namespace docstore {

class DocumentStore {
public:
  DocumentStore(const std::string &base_path, leveldb::Status &status);
  ~DocumentStore();

  // Disable copying
  DocumentStore(const DocumentStore &) = delete;
  DocumentStore &operator=(const DocumentStore &) = delete;

  leveldb::Status CreateCollection(const std::string &collection_name,
                                   leveldb::Options options);
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

  // Utility Methods
  leveldb::Status
  LoadCollectionFromRegistry(const std::string &collection_name);

  // Metadata Helper functions
  leveldb::Status CheckCollectionInRegistry(const string &collection_name);
  leveldb::Status AddCollectionToRegistry(const string &collection_name,
                                          leveldb::Options &options);

private:
  std::string base_path_;
  // TODO: change to an LRU cache
  std::unordered_map<std::string, std::unique_ptr<leveldb::DB>>
      collections_handle_;
  leveldb::Options options_;
  // This is a simple key value store which contains all collections
  std::unique_ptr<leveldb::DB> collection_registry_;
};
} // namespace docstore

#endif //  STORAGE_DOCSTORE_INCLUDE_H