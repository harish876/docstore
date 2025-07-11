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
  {
    std::lock_guard<std::mutex> lock(collections_mutex_);
    for (auto &[collection_name, filter_policy] : filter_policies_) {
      if (filter_policy) {
        delete filter_policy;
      }
    }
    filter_policies_.clear();
    collections_.clear();
  }

  // Reset the registry
  collection_registry_.reset();
}

leveldb::Status
DocumentStore::CreateCollection(const std::string &collection_name,
                                leveldb::Options options,
                                nlohmann::json &schema) {
  leveldb::Status status;
  nlohmann::json collection_metadata;
  status = CheckCollectionInRegistry(collection_name, collection_metadata);
  if (!status.IsNotFound()) {
    return OpenCollection(collection_name, collection_metadata);
  }
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

  {
    std::lock_guard<std::mutex> lock(collections_mutex_);
    collections_[collection_name] = std::unique_ptr<leveldb::DB>(db);
  }

  {
    std::lock_guard<std::mutex> lock(collections_mutex_);
    catalog_map_[collection_name] = Catalog(collection_metadata);
  }

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

  {
    std::lock_guard<std::mutex> lock(collections_mutex_);
    collections_[collection_name] = std::unique_ptr<leveldb::DB>(db);
  }

  {
    std::lock_guard<std::mutex> lock(collections_mutex_);
    catalog_map_[collection_name] = Catalog(metadata);
  }

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
  leveldb::DB *db = GetOrCreateDB(collection_name, s);
  if (!db) {
    return s;
  }
  s = db->Get(leveldb::ReadOptions(), key, &value);
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
  leveldb::DB *db = GetOrCreateDB(collection_name, s);
  if (!db) {
    return s;
  }
  s = db->Get(leveldb::ReadOptions(), secondary_key, value, top_k);
  if (!s.ok()) {
    std::cerr << "Failed to get secondary key from collection "
              << collection_name << ": " << s.ToString() << std::endl;
  }

  return s;
}

leveldb::Status DocumentStore::RangeGetSec(
    const std::string &collection_name, const std::string &secondary_start_key,
    const std::string &secondary_end_key,
    std::vector<leveldb::SecondayKeyReturnVal> *value, int top_k) {
  leveldb::Status s;
  leveldb::DB *db = GetOrCreateDB(collection_name, s);
  if (!db) {
    return s;
  }
  s = db->RangeGet(leveldb::ReadOptions(), secondary_start_key,
                   secondary_end_key, value, top_k);
  if (!s.ok()) {
    std::cerr << "Failed to range get secondary key from collection "
              << collection_name << ": " << s.ToString() << std::endl;
  }

  return s;
}

leveldb::Status DocumentStore::Insert(const std::string &collection_name,
                                      nlohmann::json &document) {

  leveldb::Status s;
  leveldb::DB *db = GetOrCreateDB(collection_name, s);
  if (!db) {
    return s;
  }
  nlohmann::json metadata;
  s = CheckCollectionInRegistry(collection_name, metadata);
  if (!s.ok()) {
    return s;
  }

  if (metadata.contains("schema")) {
    s = ValidateSchema(document, metadata["schema"]);
    if (!s.ok()) {
      return s;
    }
  }
  s = db->Put(leveldb::WriteOptions(), document.dump());
  if (!s.ok()) {
    std::cerr << "Failed to insert into collection " << collection_name << ": "
              << s.ToString() << std::endl;
    return s;
  }

  return s;
}
leveldb::Status DocumentStore::Insert(const std::string &collection_name,
                                      std::string key, std::string value) {

  leveldb::Status s;
  leveldb::DB *db = GetOrCreateDB(collection_name, s);
  if (!db) {
    return s;
  }
  s = db->Put(leveldb::WriteOptions(), key, value);
  if (!s.ok()) {
    std::cerr << "Failed to insert into collection " << collection_name << ": "
              << s.ToString() << std::endl;
  }

  return s;
}

leveldb::Status DocumentStore::ExtendMetadata(const nlohmann::json &document,
                                              const nlohmann::json &schema,
                                              nlohmann::json &new_document) {
  new_document["options"] = document;
  new_document["schema"] = schema;
  return leveldb::Status();
}

leveldb::Status DocumentStore::ValidateSchema(const nlohmann::json &document,
                                              const nlohmann::json &schema) {
  leveldb::Status s;
  if (!isValidJSON(schema)) {
    return s.Corruption("Invalid Schema Object. Invalid JSON");
  }

  Catalog catalog(schema);

  for (const auto &field_name : catalog.required) {
    if (!document.contains(field_name)) {
      char buffer[256];
      std::snprintf(buffer, sizeof(buffer),
                    "Invalid document. Missing required field: %s",
                    field_name.c_str());
      return s.InvalidArgument(buffer);
    }
  }

  for (const auto &[field_name, field_type] : catalog.fields) {
    if (!document.contains(field_name)) {
      continue;
    }

    auto document_value = document[field_name];

    if (!validate_type(document_value, const_cast<std::string &>(field_type))) {
      char buffer[256];
      std::snprintf(buffer, sizeof(buffer),
                    "Type mismatch for field '%s'. Expected type: %s.",
                    field_name.c_str(), field_type.c_str());
      return s.InvalidArgument(buffer);
    }
  }

  return s;
}

// TODO: Revisit this
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
  leveldb::DB *db = GetOrCreateDB(collection_name, s);
  if (!db) {
    return s;
  }

  std::unique_ptr<leveldb::Iterator> it(
      db->NewIterator(leveldb::ReadOptions()));

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

leveldb::Status
DocumentStore::GetAll(const std::string &collection_name,
                      std::vector<std::pair<std::string, std::string>> &kv) {
  leveldb::Status s;
  leveldb::DB *db = GetOrCreateDB(collection_name, s);
  if (!db) {
    return s;
  }

  std::unique_ptr<leveldb::Iterator> it(
      db->NewIterator(leveldb::ReadOptions()));

  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    kv.emplace_back(it->key().ToString(), it->value().ToString());
  }

  s = it->status();
  if (!s.ok()) {
    std::cerr << "Error during iteration: " << s.ToString() << std::endl;
  }

  return s;
}

leveldb::Status DocumentStore::GetRange(
    const std::string &collection_name, const std::string &start_key,
    const std::string &end_key, std::vector<nlohmann::json> &documents) {
  leveldb::Status s;
  leveldb::DB *db = GetOrCreateDB(collection_name, s);
  if (!db) {
    return s;
  }

  std::unique_ptr<leveldb::Iterator> it(
      db->NewIterator(leveldb::ReadOptions()));

  for (it->Seek(start_key); it->Valid() && it->key().ToString() <= end_key;
       it->Next()) {
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

leveldb::Status DocumentStore::GetRange(
    const std::string &collection_name, const std::string &start_key,
    const std::string &end_key,
    std::vector<std::pair<std::string, std::string>> &kv_pairs) {
  leveldb::Status s;
  leveldb::DB *db = GetOrCreateDB(collection_name, s);
  if (!db) {
    return s;
  }

  std::unique_ptr<leveldb::Iterator> it(
      db->NewIterator(leveldb::ReadOptions()));

  for (it->Seek(start_key); it->Valid() && it->key().ToString() <= end_key;
       it->Next()) {
    kv_pairs.push_back({it->key().ToString(), it->value().ToString()});
  }

  s = it->status();
  if (!s.ok()) {
    std::cerr << "Error during iteration: " << s.ToString() << std::endl;
  }

  return s;
}

leveldb::Status DocumentStore::PrefixGet(const std::string &collection_name,
                                         const std::string &prefix,
                                         std::vector<std::string> &kv_pairs) {
  leveldb::Status s;
  leveldb::DB *db = GetOrCreateDB(collection_name, s);
  if (!db) {
    return s;
  }

  std::unique_ptr<leveldb::Iterator> it(
      db->NewIterator(leveldb::ReadOptions()));

  for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix);
       it->Next()) {
    kv_pairs.push_back({it->key().ToString()});
  }

  s = it->status();
  if (!s.ok()) {
    std::cerr << "Error during iteration: " << s.ToString() << std::endl;
  }
  return s;
}

leveldb::Status DocumentStore::PrefixGet(leveldb::DB *db,
                                         const std::string &prefix,
                                         std::vector<std::string> &kv_pairs) {
  leveldb::Status s;
  std::unique_ptr<leveldb::Iterator> it(
      db->NewIterator(leveldb::ReadOptions()));

  for (it->Seek(prefix); it->Valid() && it->key().starts_with(prefix);
       it->Next()) {
    kv_pairs.push_back({it->key().ToString()});
  }

  s = it->status();
  if (!s.ok()) {
    std::cerr << "Error during iteration: " << s.ToString() << std::endl;
  }
  return s;
}

leveldb::Status
DocumentStore::GetByCompositeKey(leveldb::DB *db, const Catalog *catalog,
                                 const vector<std::string> &composite_keys,
                                 std::vector<nlohmann::json> &matched_records) {
  leveldb::Status s;

  for (const auto &composite_key : composite_keys) {
    std::string id;
    if (!DecodeCompositeKey(composite_key, catalog, id)) {
      continue;
    }
    std::string document;
    s = db->Get(leveldb::ReadOptions(), id, &document);
    if (!s.ok()) {
      std::cerr << "Failed to get document: " << s.ToString() << std::endl;
      continue;
    }
    matched_records.push_back(nlohmann::json::parse(document));
  }
  return s;
}

leveldb::Status
DocumentStore::GetByCompositeKey(const std::string &collection_name,
                                 const vector<std::string> &composite_keys,
                                 std::vector<nlohmann::json> &matched_records) {
  leveldb::Status s;

  leveldb::DB *db = GetOrCreateDB(collection_name, s);
  if (!db) {
    return s;
  }
  const Catalog *catalog = GetCatalog(collection_name);
  if (!catalog) {
    return leveldb::Status::InvalidArgument("Collection metadata not found");
  }

  for (const auto &composite_key : composite_keys) {
    std::string id;
    if (!DecodeCompositeKey(composite_key, catalog, id)) {
      continue;
    }
    std::string document;
    s = db->Get(leveldb::ReadOptions(), id, &document);
    if (!s.ok()) {
      std::cerr << "Failed to get document: " << s.ToString() << std::endl;
      continue;
    }
    matched_records.push_back(nlohmann::json::parse(document));
  }
  return s;
}

leveldb::Status
DocumentStore::Find(const std::string &collection_name,
                    const std::string &field_name,
                    const std::string &field_value,
                    std::vector<nlohmann::json> &matched_records) {
  leveldb::Status s;
  leveldb::DB *db = GetOrCreateDB(collection_name, s);
  if (!db) {
    return leveldb::Status::InvalidArgument("Database not found");
  }
  const Catalog *catalog = GetCatalog(collection_name);
  if (!catalog) {
    return leveldb::Status::InvalidArgument("Collection metadata not found");
  }

  if (field_name == catalog->primary_key) {
    std::string document;
    s = db->Get(leveldb::ReadOptions(), field_value, &document);
    if (!s.ok()) {
      std::cerr << "Failed to get document from collection " << collection_name
                << ": " << s.ToString() << std::endl;
      return s;
    }
    matched_records.push_back(nlohmann::json::parse(document));
  } else if (!catalog->secondary_key.empty() &&
             field_name == catalog->secondary_key) {
    std::vector<leveldb::SecondayKeyReturnVal> value;
    s = db->Get(leveldb::ReadOptions(), field_value, &value, 1000);

    if (!s.ok()) {
      std::cerr << "Failed to get document from collection " << collection_name
                << ": " << s.ToString() << std::endl;
      return s;
    }
    for (const auto &doc : value) {
      matched_records.push_back(nlohmann::json::parse(doc.value));
    }
  } else if (catalog->required.find(field_name) != catalog->required.end()) {
    auto field_type_it = catalog->fields.find(field_name);
    
    std::string encoded_field_value =
        EncodeFieldValue(field_value, field_type_it->second);
    std::string composite_key_prefix =
        EncodeCompositeKeyPrefix(field_name, encoded_field_value);

    std::cout << "Composite key prefix: " << composite_key_prefix << std::endl;
    std::vector<std::string> kv_pairs;
    s = PrefixGet(collection_name, composite_key_prefix, kv_pairs);
    if (!s.ok()) {
      std::cerr << "Failed to get document from collection " << collection_name
                << ": " << s.ToString() << std::endl;
      return leveldb::Status::InvalidArgument("Failed to get documents");
    }
    
  
    std::vector<std::string> composite_keys;
    for (const auto &key : kv_pairs) {
      composite_keys.push_back(key);
    }
    
    s = GetByCompositeKey(db, catalog, composite_keys, matched_records);
    if (!s.ok()) {
      std::cerr << "Failed to get document from collection " << collection_name
                << ": " << s.ToString() << std::endl;
      return leveldb::Status::InvalidArgument("Failed to get documents");
    }
    return s;
  }

  return s;
}

std::string DocumentStore::DecodeFieldValue(const std::string &encoded_value,
                                            const std::string &field_type) {
  if (field_type == "integer") {
    int32_t decoded_int;
    if (leveldb::IntEncoder::DecodeInt32(leveldb::Slice(encoded_value),
                                         &decoded_int)) {
      return std::to_string(decoded_int);
    } else {
      return "";
    }
  } else if (field_type == "string") {
    return encoded_value;
  } else {
    std::cerr << "Unsupported field type for decoding: " << field_type
              << std::endl;
    return "";
  }
}

bool DocumentStore::DecodeCompositeKey(const std::string &composite_key,
                                       const Catalog *catalog,
                                       std::string &id) {
  const std::string separator = "|";
  std::vector<std::string> parts;

  size_t pos = 0;
  size_t next_pos;

  while ((next_pos = composite_key.find(separator, pos)) != std::string::npos) {
    parts.push_back(composite_key.substr(pos, next_pos - pos));
    pos = next_pos + separator.length();
  }

  if (pos < composite_key.length()) {
    parts.push_back(composite_key.substr(pos));
  }

  if (parts.size() != 3) {
    return false;
  }

  std::string field_name = parts[0];
  std::string encoded_field_value = parts[1];
  std::string encoded_primary_key_value = parts[2];

  if (!catalog || catalog->fields.find(field_name) == catalog->fields.end()) {
    return false;
  }

  auto field_type_it = catalog->fields.find(field_name);
  if (field_type_it == catalog->fields.end()) {
    return false;
  }

  std::string primary_key_field_type = catalog->fields.at(catalog->primary_key);
  std::string decoded_primary_key_value =
      DecodeFieldValue(encoded_primary_key_value, primary_key_field_type);
  if (decoded_primary_key_value.empty()) {
    return false;
  }

  std::string field_type = field_type_it->second;
  std::string decoded_field_value =
      DecodeFieldValue(encoded_field_value, field_type);
  if (decoded_field_value.empty()) {
    return false;
  }

  id = decoded_primary_key_value;
  return true;
}

const Catalog *DocumentStore::GetCatalog(const std::string &collection_name) {
  std::lock_guard<std::mutex> lock(collections_mutex_);
  auto it = catalog_map_.find(collection_name);
  if (it != catalog_map_.end()) {
    return &it->second;
  }
  return nullptr;
}

leveldb::Status
DocumentStore::InsertWithIndex(const std::string &collection_name,
                               nlohmann::json &document) {
  leveldb::Status s;

  s = Insert(collection_name, document);
  if (!s.ok()) {
    return s;
  }

  const Catalog *catalog = GetCatalog(collection_name);
  if (!catalog) {
    return leveldb::Status::InvalidArgument("Collection metadata not found");
  }
  s = AddIndex(collection_name, document);
  return s;
}

std::string DocumentStore::EncodeFieldValue(const std::string &field_value,
                                            const std::string &field_type) {
  if (field_type == "integer") {
    int32_t int_value = std::stoi(field_value);
    return leveldb::IntEncoder::EncodeInt32(int_value);
  } else if (field_type == "string") {
    return field_value;
  } else {
    return "";
  }
}

leveldb::Status DocumentStore::AddIndex(const std::string &collection_name,
                                        const nlohmann::json &document) {
  leveldb::Status s;
  leveldb::DB *db = GetOrCreateDB(collection_name, s);
  if (!db) {
    return s;
  }

  const Catalog *catalog = GetCatalog(collection_name);
  if (!catalog) {
    return leveldb::Status::InvalidArgument("Collection metadata not found");
  }

  if (catalog->required.empty()) {
    return leveldb::Status::InvalidArgument(
        "No required fields found in schema");
  }

  std::string encoded_primary_key_value;
  if (!catalog->primary_key.empty() &&
      document.contains(catalog->primary_key)) {
    const nlohmann::json &field_value = document[catalog->primary_key];
    auto field_type_it = catalog->fields.find(catalog->primary_key);
    if (field_type_it != catalog->fields.end()) {
      encoded_primary_key_value =
          EncodeFieldValue(field_value.dump(), field_type_it->second);
    }
  }

  for (const auto &field_name : catalog->required) {
    std::string encoded_field_value;

    if (!document.contains(field_name)) {
      continue;
    }

    const nlohmann::json &field_value = document[field_name];
    auto field_type_it = catalog->fields.find(field_name);
    if (field_type_it == catalog->fields.end()) {
      continue;
    }

    if (field_type_it->second != "integer" &&
        field_type_it->second != "string") {
      continue;
    }

    encoded_field_value = EncodeFieldValue(field_value.dump(), field_type_it->second);
    if (encoded_field_value.empty()) {
      continue;
    }

    std::string composite_key = EncodeCompositeKey(
        field_name, encoded_field_value, encoded_primary_key_value);

    s = db->Put(leveldb::WriteOptions(), composite_key, "");
    if (!s.ok()) {
      std::cerr << "Failed to add index for collection " << collection_name
                << ": " << s.ToString() << std::endl;
      return s;
    }
  }
  return s;
}

std::string
DocumentStore::EncodeCompositeKey(const std::string &field_name,
                                  const std::string &encoded_field_value,
                                  const std::string &encoded_primary_key_value,
                                  const std::string &separator) {
  return field_name + separator + encoded_field_value + separator +
         encoded_primary_key_value;
}

std::string
DocumentStore::EncodeCompositeKeyPrefix(const std::string &field_name,
                                        const std::string &encoded_field_value,
                                        const std::string &separator) {
  return field_name + separator + encoded_field_value;
}

leveldb::DB *DocumentStore::GetOrCreateDB(const std::string &collection_name,
                                          leveldb::Status &s) {
  {
    std::lock_guard<std::mutex> lock(collections_mutex_);
    auto it = collections_.find(collection_name);
    if (it != collections_.end()) {
      return it->second.get();
    }
  }

  nlohmann::json metadata;
  s = CheckCollectionInRegistry(collection_name, metadata);
  if (!s.ok()) {
    return nullptr;
  }

  leveldb::Options options;
  options = options.FromJSON(metadata["options"], s);
  if (!s.ok()) {
    return nullptr;
  }

  const leveldb::FilterPolicy *filter_policy = options.filter_policy;

  leveldb::DB *db = nullptr;
  s = leveldb::DB::Open(options, base_path_ + "/" + collection_name, &db);
  if (!s.ok()) {
    std::cerr << "Failed to open collection " << collection_name << ": "
              << s.ToString() << std::endl;
    if (filter_policy) {
      delete filter_policy;
    }
    return nullptr;
  }

  {
    std::lock_guard<std::mutex> lock(collections_mutex_);
    collections_[collection_name] = std::unique_ptr<leveldb::DB>(db);
    filter_policies_[collection_name] = filter_policy;
  }

  return db;
}

} // namespace docstore