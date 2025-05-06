#include "docstore/document.h"
#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "nlohmann/json_fwd.hpp"
#include <filesystem>
#include <iostream>
#include <memory>

namespace docstore {

DocumentStore::DocumentStore(const std::string &base_path, leveldb::Status &s)
    : base_path_(base_path), collection_registry_(nullptr) {
  options_.create_if_missing = true;

  if (!std::filesystem::exists(base_path_)) {
    if (!std::filesystem::create_directories(base_path_)) {
      std::cerr << "Failed to create base directory: " << base_path_
                << std::endl;
      s = leveldb::Status::IOError(
          leveldb::Slice("Unable to create base directory"));
      return;
    }
  }

  leveldb::DB *db = nullptr;
  s = leveldb::DB::Open(options_, base_path_ + "/metadata", &db);
  if (!s.ok()) {
    std::cerr << "Failed to open metadata database: " << s.ToString()
              << std::endl;
    return;
  }

  collection_registry_ = std::unique_ptr<leveldb::DB>(db);
  // we will lazily populate the collection_ map when we recieve a
  // CreateCollection or a Insert/Update/Get query
}

leveldb::Status
DocumentStore::CreateCollection(const std::string &collection_name,
                                leveldb::Options options) {
  // explicitly set create_if_missing to true
  options.create_if_missing = true;
  leveldb::DB *db = nullptr;
  leveldb::Status status =
      leveldb::DB::Open(options, base_path_ + "/" + collection_name, &db);
  if (!status.ok()) {
    std::cerr << "Failed to create collection" << collection_name << " "
              << status.ToString() << std::endl;
    return status;
  }
  // check if collection exists in metadata table, else insert
  status = CheckCollectionInRegistry(collection_name);
  if (status.IsNotFound()) {
    status = AddCollectionToRegistry(collection_name, options);
  } else if (!status.ok()) {
    return status;
  }

  collections_handle_.insert(
      std::make_pair(collection_name, std::unique_ptr<leveldb::DB>(db)));
  return status;
}

/**
 *  - Loads a collection from the collection registry reconstructing the
 * database state from the leveldb::options metadata
 * - If collection is not found in collection registry returns a NotFound
 * leveldb::Status
 * - If collection is found and accessing the data erros then errror is
 * propagated. Check with !s.ok()
 * - If collection options are loaded and if parsing fails then a
 * InvalidArguement leveldb::Status is thrown
 * - If everything works then the constructed collection handle is added to
 * collections_handle_ map
 */
leveldb::Status
DocumentStore::LoadCollectionFromRegistry(const std::string &collection_name) {
  leveldb::Status s = CheckCollectionInRegistry(collection_name);
  if (!s.ok() || s.IsNotFound()) {
    std::cerr << "Attempt to access collection  " << collection_name
              << " which is not created " << s.ToString() << std::endl;
    return s;
  }
  leveldb::DB *db = nullptr;
  std::string serialized_options;
  s = collection_registry_->Get(leveldb::ReadOptions(), collection_name,
                                &serialized_options);
  if (!s.ok()) {
    std::cerr << "Failed to load collection  " << collection_name
              << " from registry " << s.ToString() << std::endl;
    return s;
  }

  leveldb::Options collection_options = leveldb::Options(serialized_options, s);
  if (!s.ok()) {
    std::cerr << "Failed to parse collection settings/options  "
              << collection_name << " from registry " << s.ToString()
              << std::endl;
    return s;
  }

  s = leveldb::DB::Open(collection_options, base_path_ + "/" + collection_name,
                        &db);
  if (!s.ok()) {
    std::cerr << "Unable to open registered collection " << collection_name
              << " " << s.ToString() << std::endl;
    return s;
  }

  collections_handle_.insert(
      std::make_pair(collection_name, std::unique_ptr<leveldb::DB>(db)));
  return s;
}

leveldb::Status
DocumentStore::CheckCollectionInRegistry(const std::string &collection_name) {
  assert(metadata_db_ != nullptr);
  std::string value;
  leveldb::Status s = collection_registry_->Get(leveldb::ReadOptions(),
                                                collection_name, &value);
  if (!s.ok()) {
    return s;
  }

  if (value.empty()) {
    return leveldb::Status::NotFound("Collection Name" + collection_name +
                                     "Not Found in metdata table");
  }
  return s;
}

leveldb::Status
DocumentStore::AddCollectionToRegistry(const std::string &collection_name,
                                       leveldb::Options &options) {
  assert(metadata_db_ != nullptr);
  nlohmann::json serialized_json = options.ToJSON();
  return collection_registry_->Put(leveldb::WriteOptions(), collection_name,
                                   serialized_json.dump());
}

leveldb::Status DocumentStore::Get(const std::string &collection_name,
                                   std::string key, std::string &value) {
  leveldb::Status s;
  std::unique_ptr<leveldb::DB> *collection_db =
      GetCollectionHandle(collection_name, s);
  if (!collection_db) {
    return s;
  }
  s = (*collection_db)->Get(leveldb::ReadOptions(), key, &value);
  if (!s.ok()) {
    std::cerr << "Failed to get from collection " << collection_name << ": "
              << s.ToString() << std::endl;
  }
  return s;
}

leveldb::Status DocumentStore::GetSec(
    const std::string &collection_name, const std::string &secondary_key,
    std::vector<leveldb::SecondayKeyReturnVal> *value, int top_k) {
  leveldb::Status s;
  std::unique_ptr<leveldb::DB> *collection_db =
      GetCollectionHandle(collection_name, s);
  if (!collection_db) {
    return s;
  }
  s = (*collection_db)
          ->Get(leveldb::ReadOptions(), secondary_key, value, top_k);
  if (!s.ok()) {
    std::cerr << "Failed to insert into collection " << collection_name << ": "
              << s.ToString() << std::endl;
  }
  return s;
}

leveldb::Status DocumentStore::RangeGetSec(
    const std::string &collection_name, const std::string &secondary_start_key,
    const std::string &secondary_end_key,
    std::vector<leveldb::SecondayKeyReturnVal> *value, int top_k) {
  leveldb::Status s;
  std::unique_ptr<leveldb::DB> *collection_db =
      GetCollectionHandle(collection_name, s);
  if (!collection_db) {
    return s;
  }
  s = (*collection_db)
          ->RangeGet(leveldb::ReadOptions(), secondary_start_key,
                     secondary_end_key, value, top_k);
  if (!s.ok()) {
    std::cerr << "Failed to insert into collection " << collection_name << ": "
              << s.ToString() << std::endl;
  }
  return s;
}

// TODO: Columnar decomposition
leveldb::Status DocumentStore::Insert(const std::string &collection_name,
                                      nlohmann::json &document) {

  leveldb::Status s;
  std::unique_ptr<leveldb::DB> *collection_db =
      GetCollectionHandle(collection_name, s);
  if (!collection_db) {
    return s;
  }
  s = (*collection_db)->Put(leveldb::WriteOptions(), document.dump());
  if (!s.ok()) {
    std::cerr << "Failed to insert into collection " << collection_name << ": "
              << s.ToString() << std::endl;
  }
  return s;
}

// TODO: Columnar decomposition
leveldb::Status DocumentStore::Insert(const std::string &collection_name,
                                      std::string key, std::string value) {
  leveldb::Status s;
  std::unique_ptr<leveldb::DB> *collection_db =
      GetCollectionHandle(collection_name, s);
  if (!collection_db) {
    return s;
  }
  s = (*collection_db)->Put(leveldb::WriteOptions(), key, value);
  if (!s.ok()) {
    std::cerr << "Failed to insert into collection " << collection_name << ": "
              << s.ToString() << std::endl;
  }
  return s;
}

std::unique_ptr<leveldb::DB> *
DocumentStore::GetCollectionHandle(const std::string &collection_name,
                                   leveldb::Status &s) {

  auto it = collections_handle_.find(collection_name);
  if (it != collections_handle_.end()) {
    return &it->second;
  }

  s = CheckCollectionInRegistry(collection_name);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      std::cerr << "Attempt to access collection " << collection_name
                << " which is not created " << s.ToString() << std::endl;
    }
    return nullptr;
  }

  s = LoadCollectionFromRegistry(collection_name);
  if (!s.ok()) {
    return nullptr;
  }

  it = collections_handle_.find(collection_name);
  if (it != collections_handle_.end()) {
    return &it->second;
  }

  return nullptr;
}

DocumentStore::~DocumentStore() {}

} // namespace docstore