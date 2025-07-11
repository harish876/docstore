#ifndef STORAGE_DOCSTORE_INCLUDE_H
#define STORAGE_DOCSTORE_INCLUDE_H

#include "leveldb/options.h"
#include "leveldb/status.h"
#include "memory"
#include "nlohmann/json.hpp"
#include "util/encoding.h"
#include <iostream>
#include <leveldb/db.h>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace docstore {

struct Catalog {
  std::unordered_map<std::string, std::string>
      fields; // field_name -> field_type
  std::set<std::string> required;
  std::string primary_key;
  std::string secondary_key;

  Catalog() = default;

  Catalog(const nlohmann::json &metadata_json) {
    if (metadata_json.contains("options") && metadata_json.contains("schema")) {
      if (metadata_json["options"].contains("primary_key")) {
        primary_key = metadata_json["options"]["primary_key"];
      }
      if (metadata_json["options"].contains("secondary_key")) {
        secondary_key = metadata_json["options"]["secondary_key"];
      }

      if (metadata_json["schema"].contains("fields")) {
        for (const auto &field_def : metadata_json["schema"]["fields"]) {
          for (const auto &[field_name, field_type] : field_def.items()) {
            fields[field_name] = field_type.get<std::string>();
          }
        }
      }

      if (metadata_json["schema"].contains("required")) {
        for (const auto &field : metadata_json["schema"]["required"]) {
          required.insert(field.get<std::string>());
        }
      }
    } else {
      if (metadata_json.contains("fields")) {
        for (const auto &field_def : metadata_json["fields"]) {
          for (const auto &[field_name, field_type] : field_def.items()) {
            fields[field_name] = field_type.get<std::string>();
          }
        }
      }

      if (metadata_json.contains("required")) {
        for (const auto &field : metadata_json["required"]) {
          required.insert(field.get<std::string>());
        }
      }
    }
  }
};

struct CollectionHandle {
  std::unique_ptr<leveldb::DB> db_;
  nlohmann::json metadata_;
  Catalog catalog_;

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

  // Get Methods
  // Generic Find Method integrating all other Get/Find Methods
  leveldb::Status Find(const std::string &collection_name,
                       const std::string &field_name,
                       const nlohmann::json &field_value,
                       std::vector<nlohmann::json> &matched_records);
  leveldb::Status Get(const std::string &collection_name, std::string key,
                      std::string &value);
  leveldb::Status GetSec(const std::string &collection_name,
                         const std::string &secondary_key,
                         std::vector<leveldb::SecondayKeyReturnVal> *value,
                         int top_k = 1000);
  leveldb::Status
  GetByCompositeKey(const std::string &collection_name,
                    const vector<std::string> &composite_keys,
                    std::vector<nlohmann::json> &matched_records);
  leveldb::Status
  GetByCompositeKey(leveldb::DB *db, const Catalog *catalog,
                    const vector<std::string> &composite_keys,
                    std::vector<nlohmann::json> &matched_records);
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
  leveldb::DB *GetOrCreateDB(const std::string &collection_name,
                             leveldb::Status &s);

  // Metadata Helper functions
  leveldb::Status CheckCollectionInRegistry(const string &collection_name,
                                            nlohmann::json &metadata);
  leveldb::Status AddCollectionToRegistry(const string &collection_name,
                                          nlohmann::json &metadata);
  leveldb::Status OpenCollection(const std::string &collection_name,
                                 const nlohmann::json &metadata);
  leveldb::Status ValidateSchema(const nlohmann::json &document,
                                 const nlohmann::json &schema);

  const Catalog *GetCatalog(const std::string &collection_name);

  leveldb::Status GetAll(const std::string &collection_name,
                         std::vector<nlohmann::json> &documents);
  leveldb::Status GetAll(const std::string &collection_name,
                         std::vector<std::pair<std::string, std::string>> &kv);
  leveldb::Status GetRange(const std::string &collection_name,
                           const std::string &min_key,
                           const std::string &max_key,
                           std::vector<nlohmann::json> &documents);

  leveldb::Status
  GetRange(const std::string &collection_name, const std::string &min_key,
           const std::string &max_key,
           std::vector<std::pair<std::string, std::string>> &kv_pairs);
  leveldb::Status PrefixGet(const std::string &collection_name,
                            const std::string &prefix,
                            std::vector<std::string> &matched_keys);
  leveldb::Status PrefixGet(leveldb::DB *db, const std::string &prefix,
                            std::vector<std::string> &matched_keys);

  // Composite Index Methods
  leveldb::Status AddIndex(const std::string &collection_name,
                           const nlohmann::json &document);
  leveldb::Status InsertWithIndex(const std::string &collection_name,
                                  nlohmann::json &document);
  std::string EncodeCompositeKey(const std::string &field_name,
                                 const std::string &encoded_field_value,
                                 const std::string &encoded_primary_key_value,
                                 const std::string &separator = "|");

  std::string EncodeCompositeKeyPrefix(const std::string &field_name,
                                       const std::string &encoded_field_value,
                                       const std::string &separator = "|");

  std::string DecodeFieldValue(const std::string &encoded_value,
                               const std::string &field_type);
  bool DecodeCompositeKey(const std::string &composite_key,
                          const Catalog *catalog, std::string &id);
  std::string EncodeFieldValue(const nlohmann::json &field_value,
                               const std::string &field_type);

private:
  std::string base_path_;
  std::unique_ptr<leveldb::DB> collection_registry_;
  std::unordered_map<std::string, std::unique_ptr<leveldb::DB>> collections_;
  std::unordered_map<std::string, const leveldb::FilterPolicy *>
      filter_policies_;
  std::unordered_map<std::string, Catalog> catalog_map_;
  std::mutex collections_mutex_;
  leveldb::Options options_;

  bool validate_type(nlohmann::basic_json<> &document_value, std::string &type);
};

} // namespace docstore

#endif //  STORAGE_DOCSTORE_INCLUDE_H