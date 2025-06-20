#include "docstore/document.h"
#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include <cassert>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>

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

DocumentStore::~DocumentStore() {
  collections_handle_.clear();
  collection_registry_.reset();
}

/**
  collection_name is mandatory
  options is mandatory and resolves to default leveldb options
  schema is non mandatory and if left emtpy turns collection into a vanilla KV
  store
 */
leveldb::Status
DocumentStore::CreateCollection(const std::string &collection_name,
                                leveldb::Options options,
                                nlohmann::json &schema) {
  // check if collection exists in metadata table. if exists dont do anything
  leveldb::Status status;
  nlohmann::json collection_metadata;
  status = CheckCollectionInRegistry(collection_name, collection_metadata);
  if (!status.IsNotFound()) {
    return OpenCollection(collection_name, collection_metadata);
  }

  // create the database
  options.create_if_missing = true;
  leveldb::DB *db = nullptr;
  status = leveldb::DB::Open(options, base_path_ + "/" + collection_name, &db);
  if (!status.ok()) {
    std::cerr << "Failed to create collection " << collection_name << " "
              << status.ToString() << std::endl;
    return status;
  }

  nlohmann::json s_options = options.ToJSON();
  status = ExtendMetadata(s_options, schema, collection_metadata);
  if (!status.ok()) {
    std::cerr << "Error at ExtendMetata at CheckCollectionToRegistry "
              << collection_name << std::endl;
    delete db;
    return status;
  }

  status = AddCollectionToRegistry(collection_name, collection_metadata);
  if (!status.ok()) {
    std::cerr << "Error at AddCollectionToRegistry " << collection_name
              << std::endl;
    delete db;
    return status;
  }

  collections_handle_.emplace(
      collection_name,
      CollectionHandle{std::unique_ptr<leveldb::DB>(db), collection_metadata});
  return status;
}

leveldb::Status
DocumentStore::DropCollection(const std::string &collection_name) {
  return leveldb::Status::NotSupported(
      "Dropping collection not implemented yet");
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
DocumentStore::OpenCollection(const std::string &collection_name,
                              const nlohmann::json &metadata) {
  // create the database
  leveldb::Options collection_options;
  leveldb::Status status;
  collection_options = collection_options.FromJSON(metadata["options"], status);
  collection_options.create_if_missing = false; // disable this

  if (!status.ok()) {
    std::cerr << "Failed to parse collection settings/options  "
              << collection_name << " from registry " << status.ToString()
              << std::endl;
    return status;
  }

  leveldb::DB *db = nullptr;
  status = leveldb::DB::Open(collection_options,
                             base_path_ + "/" + collection_name, &db);
  if (!status.ok()) {
    std::cerr << "Unable to open registered collection " << collection_name
              << " " << status.ToString() << std::endl;
    return status;
  }

  collections_handle_.emplace(
      collection_name,
      CollectionHandle{std::unique_ptr<leveldb::DB>(db), metadata});
  
  return status;
}

leveldb::Status
DocumentStore::CheckCollectionInRegistry(const std::string &collection_name,
                                         nlohmann::json &metadata) {
  assert(collection_registry_ != nullptr);
  std::string metadata_buf;
  leveldb::Status s = collection_registry_->Get(leveldb::ReadOptions(),
                                                collection_name, &metadata_buf);
  if (!s.ok()) {
    return s;
  }
  if (metadata_buf.empty()) {
    return leveldb::Status::NotFound("Collection Name " + collection_name +
                                     "Not Found in metdata table");
  }
  try {
    auto parse_result = nlohmann::json::parse(metadata_buf, nullptr, false);
    if (parse_result.is_discarded()) {
      return leveldb::Status::NotFound("Invalid metadata stored - ",
                                       collection_name);
    }
    metadata = parse_result;
  } catch (const nlohmann::json::parse_error &e) {
    return leveldb::Status::NotFound("Invalid metadata stored - ",
                                     collection_name);
  }
  return s;
}

leveldb::Status
DocumentStore::AddCollectionToRegistry(const std::string &collection_name,
                                       nlohmann::json &metadata) {
  assert(collection_registry_ != nullptr);
  leveldb::Status s = collection_registry_->Put(
      leveldb::WriteOptions(), collection_name, metadata.dump());
  if (!s.ok()) {
    std::cerr << "Error at AddCollectionToRegistry " << s.ToString() << " \n";
  }
  return s;
}

leveldb::Status DocumentStore::Get(const std::string &collection_name,
                                   std::string key, std::string &value) {

  leveldb::Status s;
  CollectionHandle *collection_handle = GetCollectionHandle(collection_name, s);
  if (!collection_handle) {
    return s;
  }
  s = (collection_handle->db_)->Get(leveldb::ReadOptions(), key, &value);
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
  CollectionHandle *collection_db = GetCollectionHandle(collection_name, s);
  if (!collection_db) {
    std::cout << "Collection handle not found in registry. This should not happen" << std::endl;
    return s.NotFound(
        "Collection handle not found in registry. This should not happen");
  }
  s = (collection_db->db_)
          ->Get(leveldb::ReadOptions(), secondary_key, value, top_k);
  if (!s.ok()) {
    std::cerr << "Failed to get secondary key from collection " << collection_name << ": "
              << s.ToString() << std::endl;
  }
  return s;
}

leveldb::Status DocumentStore::RangeGetSec(
    const std::string &collection_name, const std::string &secondary_start_key,
    const std::string &secondary_end_key,
    std::vector<leveldb::SecondayKeyReturnVal> *value, int top_k) {
  leveldb::Status s;
  CollectionHandle *collection_db = GetCollectionHandle(collection_name, s);
  if (!collection_db || (collection_db && !collection_db->db_)) {
    return s.NotFound(
        "Collection handle not found in registry. This should not happen");
  }
  s = (collection_db->db_)
          ->RangeGet(leveldb::ReadOptions(), secondary_start_key,
                     secondary_end_key, value, top_k);
  if (!s.ok()) {
    std::cerr << "Failed to range get secondary key from collection " << collection_name << ": "
              << s.ToString() << std::endl;
  }
  return s;
}

// TODO: Columnar decomposition
leveldb::Status DocumentStore::Insert(const std::string &collection_name,
                                      nlohmann::json &document) {

  leveldb::Status s;
  CollectionHandle *collection_handle = GetCollectionHandle(collection_name, s);
  if (!collection_handle || (collection_handle && !collection_handle->db_)) {
    return s.NotFound(
        "Collection handle not found in registry. This should not happen");
  }
  if (collection_handle->ApplySchemaCheck()) {
    s = ValidateSchema(document, collection_handle->metadata_["schema"]);
    if (!s.ok()) {
      return s;
    }
  }

  s = (collection_handle->db_)->Put(leveldb::WriteOptions(), document.dump());
  if (!s.ok()) {
    std::cerr << "Failed to insert into collection " << collection_name << ": "
              << s.ToString() << std::endl;
    return s;
  }
  return s;
}

// TODO: Columnar decomposition
leveldb::Status DocumentStore::Insert(const std::string &collection_name,
                                      std::string key, std::string value) {

  leveldb::Status s;
  CollectionHandle *collection_handle = GetCollectionHandle(collection_name, s);
  if (!collection_handle || (collection_handle && !collection_handle->db_)) {
    return s.NotFound(
        "Collection handle not found in registry. This should not happen");
  }
  s = (collection_handle->db_)->Put(leveldb::WriteOptions(), key, value);
  if (!s.ok()) {
    std::cerr << "Failed to insert into collection " << collection_name << ": "
              << s.ToString() << std::endl;
  }
  return s;
}

CollectionHandle *
DocumentStore::GetCollectionHandle(const std::string &collection_name,
                                   leveldb::Status &s) {
  auto it = collections_handle_.find(collection_name);
  if (it != collections_handle_.end()) {
    return &it->second;
  }

  nlohmann::json metadata;
  s = CheckCollectionInRegistry(collection_name, metadata);
  if (!s.ok()) {
    std::cerr << "Failed to get collection metadata " << collection_name << ": "
              << s.ToString() << std::endl;
    return nullptr;
  }

  s = OpenCollection(collection_name, metadata);
  if (!s.ok()) {
    std::cerr << "Failed to open collection " << collection_name << ": "
              << s.ToString() << std::endl;
    return nullptr;
  }

  it = collections_handle_.find(collection_name);
  if (it != collections_handle_.end()) {
    return &it->second;
  }
  return nullptr;
}

leveldb::Status DocumentStore::ExtendMetadata(const nlohmann::json &document,
                                              const nlohmann::json &schema,
                                              nlohmann::json &new_document) {
  new_document["options"] = document;
  new_document["schema"] = schema;
  return leveldb::Status();
}

/**
  Supports
    - string
    - integer
    - boolean
    - array [Non Recursive Check]
    - null
 */
leveldb::Status DocumentStore::ValidateSchema(const nlohmann::json &document,
                                              const nlohmann::json &schema) {
  leveldb::Status s;
  if (!isValidJSON(schema)) {
    return s.Corruption("Invalid Schema Object. Invalid JSON");
  }
  /**
    Example Schema Object. Extend support only for data types
    json schema = {
      {"fields",
       [
           {"id", "string"},
           {"name", "string"},
           {"age", "integer"},
           {"is_active","boolean"},
       ],
      {"required", ["id", "name", "age", "is_active"]},
  };
  */
  if (!schema.contains("required") ||
      (schema.contains("required") && !schema["required"].is_array())) {
    return s.Corruption("Invalid Schema Object. 'required' field missing");
  }

  nlohmann::json::array_t required_fields = schema["required"];
  for (auto field : required_fields) {
    if (!document.contains(field.get<std::string>().c_str())) {
      char buffer[256];
      std::snprintf(buffer, sizeof(buffer),
                    "Invalid document. Missing required field: %s",
                    field.get<std::string>().c_str());
      return s.InvalidArgument(buffer);
    }
  }

  if (!schema.contains("fields") ||
      (schema.contains("fields") && !schema["fields"].is_array())) {
    return s.Corruption("Invalid Schema Object. 'fields' field missing");
  }

  nlohmann::json::array_t fields = schema["fields"];
  for (const auto &field : fields) {
    for (auto &[field_name, field_type] : field.items()) {
      if (!document.contains(field_name)) {
        continue;
      }

      auto document_value = document[field_name];

      std::string field_type_str = field_type.get<std::string>();
      if (!validate_type(document_value, field_type_str)) {
        char buffer[256];
        std::snprintf(buffer, sizeof(buffer),
                      "Type mismatch for field '%s'. Expected type: %s.",
                      field_name.c_str(),
                      field_type.get<std::string>().c_str());
        return s.InvalidArgument(buffer);
      }
    }
  }

  return s;
}

bool DocumentStore::validate_type(nlohmann::basic_json<> &document_value,
                                  std::string &type) {
  if (type == "string") {
    return document_value.is_string();
  } else if (type == "integer") {
    return document_value.is_number_integer();
  } else if (type == "boolean") {
    return document_value.is_boolean();
  } else if (type == "array") {
    return document_value.is_array();
  } else if (type == "null") {
    return document_value.is_null();
  } else {
    return false;
  }
}

bool DocumentStore::isValidJSON(const nlohmann::json &document) {
  try {
    auto parse_result = nlohmann::json::parse(document.dump(), nullptr, false);
    return !parse_result.is_discarded();
  } catch (const nlohmann::json::parse_error &e) {
    return false;
  }
}

leveldb::Status DocumentStore::GetAll(const std::string &collection_name,
                                      std::vector<nlohmann::json> &documents) {
  leveldb::Status s;
  CollectionHandle *collection_handle = GetCollectionHandle(collection_name, s);
  if (!collection_handle || (collection_handle && !collection_handle->db_)) {
    return s.NotFound("Collection handle not found in registry");
  }

  std::unique_ptr<leveldb::Iterator> it(
      collection_handle->db_->NewIterator(leveldb::ReadOptions()));

  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    nlohmann::json doc;
    auto parse_result =
        nlohmann::json::parse(it->value().ToString(), nullptr, false);
    if (parse_result.is_discarded()) {
      std::cerr << "Failed to parse document" << std::endl;
      continue;
    }
    documents.push_back(parse_result);
  }

  s = it->status();
  if (!s.ok()) {
    std::cerr << "Error during iteration: " << s.ToString() << std::endl;
  }

  return s;
}

leveldb::Status DocumentStore::GetRange(const std::string &collection_name,
                                        const std::string &start_key,
                                        const std::string &end_key,
                                        std::vector<nlohmann::json> &documents) {
  leveldb::Status s;
  CollectionHandle *collection_handle = GetCollectionHandle(collection_name, s);
  if (!collection_handle || (collection_handle && !collection_handle->db_)) {
    return s.NotFound("Collection handle not found in registry");
  }

  std::unique_ptr<leveldb::Iterator> it(
      collection_handle->db_->NewIterator(leveldb::ReadOptions()));

  for (it->Seek(start_key); it->Valid() && it->key().ToString() <= end_key; it->Next()) {
    nlohmann::json doc;
    auto parse_result =
        nlohmann::json::parse(it->value().ToString(), nullptr, false);
    if (parse_result.is_discarded()) {
      std::cerr << "Failed to parse document" << std::endl;
      continue;
    }
    documents.push_back(parse_result);
  }

  s = it->status();
  if (!s.ok()) {
    std::cerr << "Error during iteration: " << s.ToString() << std::endl;
  }

  return s;
}

} // namespace docstore