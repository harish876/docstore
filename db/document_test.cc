#include "docstore/document.h"
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "nlohmann/json_fwd.hpp"
#include "util/testharness.h"
#include <cassert>
#include <iostream>
#include <nlohmann/json.hpp>

class DocumentStoreTest {
public:
  std::string test_dir_ = "/root/docstore/test";
  void TearDown() {
    if (std::filesystem::exists(test_dir_)) {
      std::filesystem::remove_all(test_dir_);
    }
  }
};

namespace docstore {

TEST(DocumentStoreTest, SerializeOptions) {
  leveldb::Options options;
  options.primary_key = "id";
  options.secondary_key = "__sec_attr";
  nlohmann::json serialized_ops = options.ToJSON();
  ASSERT_TRUE(serialized_ops.contains("primary_key") &&
              serialized_ops["primary_key"] == "id");
  ASSERT_TRUE(serialized_ops.contains("secondary_key") &&
              serialized_ops["secondary_key"] == "__sec_attr");
  ASSERT_TRUE(!serialized_ops.contains("filter_policy"));

  this->TearDown();
}

TEST(DocumentStoreTest, ParseOptionsFromJSON) {
  nlohmann::json schema;
  nlohmann::json s_options;
  s_options = R"(
    {
      "primary_key": "id",
      "secondary_key": "age"
    }
  )"_json;

  leveldb::Status s;
  leveldb::Options options;
  options = options.FromJSON(s_options, s);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(options.primary_key == "id");
  ASSERT_TRUE(options.secondary_key == "age");

  docstore::DocumentStore store(this->test_dir_, s);
  ASSERT_TRUE(s.ok());

  s = store.CreateCollection("users", options, schema);
  ASSERT_TRUE(s.ok());
  this->TearDown();
}

TEST(DocumentStoreTest, SerializeBloomFilter) {
  leveldb::Options options;
  options.filter_policy = leveldb::NewBloomFilterPolicy(20);
  nlohmann::json serialized_ops = options.ToJSON();
  ASSERT_TRUE(serialized_ops.contains("filter_policy") &&
              serialized_ops["filter_policy"] == "leveldb.BuiltinBloomFilter");

  this->TearDown();
}

TEST(DocumentStoreTest, CreateCollection) {
  leveldb::Status s;
  docstore::DocumentStore store(this->test_dir_, s);
  ASSERT_TRUE(s.ok());

  leveldb::Options options;
  nlohmann::json empty_schema;

  s = store.CreateCollection("users", options, empty_schema);
  ASSERT_TRUE(s.ok());

  nlohmann::json metadata;
  ASSERT_TRUE(store.CheckCollectionInRegistry("users", metadata).ok());

  this->TearDown();
}

TEST(DocumentStoreTest, CreateCollectionWithSchema) {
  leveldb::Status s;
  docstore::DocumentStore store(this->test_dir_, s);
  ASSERT_TRUE(s.ok());

  leveldb::Options options;
  nlohmann::json schema;
  schema = R"(
    {
      "fields": [
        {"user_id": "integer"},
        {"user_age": "integer"},
        {"user_name": "string"}
      ],
      "required": [ "user_id", "user_age"]
    }
  )"_json;

  store.CreateCollection("users_1", options, schema);
  ASSERT_TRUE(s.ok());

  nlohmann::json metadata;
  ASSERT_TRUE(store.CheckCollectionInRegistry("users_1", metadata).ok());

  this->TearDown();
}

TEST(DocumentStoreTest, QueryOnNonExistentCollection) {
  leveldb::Status s;
  docstore::DocumentStore store(this->test_dir_, s);
  leveldb::Options options;
  ASSERT_TRUE(s.ok());
  nlohmann::json metadata;
  s = store.CheckCollectionInRegistry("users", metadata);
  ASSERT_TRUE(!s.ok());

  this->TearDown();
}

TEST(DocumentStoreTest, ValidateSchema) {
  leveldb::Status s;
  docstore::DocumentStore store(this->test_dir_, s);
  ASSERT_TRUE(s.ok());
  leveldb::Options options;

  nlohmann::json schema;
  schema = R"(
    {
      "fields": [
        {"user_id": "integer"},
        {"user_age": "integer"},
        {"user_name": "string"}
      ],
      "required": [ "user_id", "user_age"]
    }
  )"_json;

  nlohmann::json doc;
  doc["user_id"] = 1;
  doc["user_age"] = 20;
  doc["user_name"] = "user_1";

  store.ValidateSchema(doc, schema);

  ASSERT_TRUE(s.ok());

  this->TearDown();
}

TEST(DocumentStoreTest, ValidateFalseSchema) {
  leveldb::Status s;
  docstore::DocumentStore store(this->test_dir_, s);
  ASSERT_TRUE(s.ok());
  leveldb::Options options;

  nlohmann::json schema;
  schema = R"(
    {
      "fields": [
        {"user_id": "integer"},
        {"user_age": "integer"},
        {"user_name": "string"}
      ],
      "required": [ "user_id", "user_age"]
    }
  )"_json;

  nlohmann::json doc;
  doc["user_id"] = 1;
  doc["user_name"] = "user_1";

  s = store.ValidateSchema(doc, schema);

  ASSERT_TRUE(!s.ok());
  std::cout << "Invalid Schema Message - " << s.ToString() << " \n";

  this->TearDown();
}

TEST(DocumentStoreTest, ExtendMetadataTest) {
  leveldb::Status s;
  docstore::DocumentStore store(this->test_dir_, s);
  ASSERT_TRUE(s.ok());
  leveldb::Options options;
  options.primary_key = "user_id";
  options.secondary_key = "user_age";

  nlohmann::json schema;
  schema = R"(
    {
      "fields": [
        {"user_id": "integer"},
        {"user_age": "integer"},
        {"user_name": "string"}
      ],
      "required": [ "user_id", "user_age"]
    }
  )"_json;

  nlohmann::json s_options = options.ToJSON();
  nlohmann::json result;

  store.ExtendMetadata(s_options, schema, result);

  ASSERT_TRUE(result.contains("options"));
  ASSERT_TRUE(result.contains("schema"));
}

TEST(DocumentStoreTest, CheckCollectionInRegistryTest) {
  leveldb::Status s;
  docstore::DocumentStore store(this->test_dir_, s);
  ASSERT_TRUE(s.ok());
  leveldb::Options options;
  options.primary_key = "user_id";
  options.secondary_key = "user_age";

  nlohmann::json schema;
  schema = R"(
    {
      "fields": [
        {"user_id": "integer"},
        {"user_age": "integer"},
        {"user_name": "string"}
      ],
      "required": [ "user_id", "user_age"]
    }
  )"_json;

  s = store.CreateCollection("users", options, schema);

  nlohmann::json metadata;
  s = store.CheckCollectionInRegistry("users", metadata);
  ASSERT_OK(s);

  ASSERT_TRUE(metadata.contains("options"));
  ASSERT_TRUE(metadata.contains("schema"));

  this->TearDown();
}

TEST(DocumentStoreTest, GetCollectionHandleTest) {
  leveldb::Status s;
  docstore::DocumentStore store(this->test_dir_, s);
  ASSERT_TRUE(s.ok());
  leveldb::Options options;
  options.primary_key = "user_id";
  options.secondary_key = "user_age";

  nlohmann::json schema;
  schema = R"(
    {
      "fields": [
        {"user_id": "integer"},
        {"user_age": "integer"},
        {"user_name": "string"}
      ],
      "required": [ "user_id", "user_age"]
    }
  )"_json;

  s = store.CreateCollection("users", options, schema);
  auto handle = store.GetCollectionHandle("users", s);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(handle != nullptr);

  ASSERT_TRUE(handle->metadata_.contains("options"));
  ASSERT_TRUE(handle->metadata_.contains("schema"));

  nlohmann::json doc;
  doc["user_id"] = 1;
  doc["user_name"] = "user_1";

  s = store.Insert("users", doc);

  ASSERT_TRUE(!s.ok());

  this->TearDown();
}

TEST(DocumentStoreTest, PutDocument) {
  leveldb::Status s;
  docstore::DocumentStore store(this->test_dir_, s);
  ASSERT_TRUE(s.ok());
  leveldb::Options options;
  options.primary_key = "user_id";
  options.secondary_key = "user_age";

  nlohmann::json schema;
  schema = R"(
    {
      "fields": [
        {"user_id": "integer"},
        {"user_age": "integer"},
        {"user_name": "string"}
      ],
      "required": [ "user_id", "user_age"]
    }
  )"_json;

  s = store.CreateCollection("users", options, schema);
  ASSERT_TRUE(s.ok());

  nlohmann::json doc;
  doc["user_id"] = 1;
  doc["user_name"] = "user_1";

  s = store.Insert("users", doc);

  ASSERT_TRUE(!s.ok());

  this->TearDown();
}

TEST(DocumentStoreTest, PutAndGetQueryNormal) {
  leveldb::Status s;
  docstore::DocumentStore store(this->test_dir_, s);
  ASSERT_TRUE(s.ok());

  leveldb::Options options;
  nlohmann::json empty_schema;

  s = store.CreateCollection("users", options, empty_schema);
  ASSERT_TRUE(s.ok());

  store.Insert("users", "harish", "yayy");
  ASSERT_TRUE(s.ok());

  std::string value;
  s = store.Get("users", "harish", value);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(value == "yayy");

  this->TearDown();
}

TEST(DocumentStoreTest, PutAndGetQueryDocument) {
  leveldb::Status s;
  docstore::DocumentStore store(this->test_dir_, s);
  ASSERT_TRUE(s.ok());

  leveldb::Options options;

  options.primary_key = "user_id";
  options.secondary_key = "user_age";
  options.filter_policy = leveldb::NewBloomFilterPolicy(20);
  nlohmann::json schema;
  schema = R"(
    {
      "fields": [
        {"user_id": "integer"},
        {"user_age": "integer"},
        {"user_name": "string"}
      ],
      "required": [ "user_id", "user_age"]
    }
  )"_json;

  s = store.CreateCollection("users", options, schema);
  ASSERT_TRUE(s.ok());

  for (int i = 1; i <= 10; ++i) {
    nlohmann::json doc;
    doc["user_id"] = i;
    doc["user_age"] = 20 + i;
    doc["user_name"] = "user_" + std::to_string(i);

    s = store.Insert("users", doc);
    ASSERT_TRUE(s.ok());
  }

  std::string value;
  s = store.Get("users", "5", value);
  ASSERT_TRUE(s.ok());

  nlohmann::json retrieved_doc = nlohmann::json::parse(value);
  ASSERT_TRUE(retrieved_doc["user_age"] == 25);
  ASSERT_TRUE(retrieved_doc["user_name"] == "user_5");

  this->TearDown();
}

TEST(DocumentStoreTest, PutAndGetQueryDocumentWithSecIndex) {
  leveldb::Status s;
  docstore::DocumentStore store(this->test_dir_, s);
  ASSERT_TRUE(s.ok());

  leveldb::Options options;
  options.primary_key = "user_id";
  options.secondary_key = "user_age";
  options.filter_policy = leveldb::NewBloomFilterPolicy(20);

  nlohmann::json schema;
  schema = R"(
    {
      "fields": [
        {"user_id": "integer"},
        {"user_age": "integer"},
        {"user_name": "string"}
      ],
      "required": [ "user_id", "user_age"]
    }
  )"_json;

  s = store.CreateCollection("users", options, schema);
  ASSERT_TRUE(s.ok());

  for (int i = 1; i <= 10; ++i) {
    nlohmann::json doc;
    doc["user_id"] = i;
    doc["user_age"] = 20 + i;
    doc["user_name"] = "user_" + std::to_string(i);

    s = store.Insert("users", doc);
    ASSERT_TRUE(s.ok());
  }

  std::vector<leveldb::SecondayKeyReturnVal> secondary_values;
  s = store.GetSec("users", "30", &secondary_values, 1000);
  ASSERT_TRUE(s.ok());
  ASSERT_TRUE(secondary_values.size() == 1);

  secondary_values.clear();
  s = store.RangeGetSec("users", "21", "30", &secondary_values, 1000);
  ASSERT_TRUE(s.ok());

  ASSERT_TRUE(secondary_values.size() == 10);

  this->TearDown();
}

TEST(DocumentStoreTest, PersistAndRetrieveCollection) {
  leveldb::Status s;
  {
    docstore::DocumentStore store(this->test_dir_, s);
    ASSERT_TRUE(s.ok());

    // Define options and schema
    leveldb::Options options;
    options.primary_key = "user_id";
    options.secondary_key = "user_age";

    nlohmann::json schema;
    schema = R"(
      {
        "fields": [
          {"user_id": "integer"},
          {"user_age": "integer"},
          {"user_name": "string"}
        ],
        "required": [ "user_id", "user_age"]
      }
    )"_json;

    // Create the "users" collection
    s = store.CreateCollection("users", options, schema);
    s = store.CreateCollection("users_1", options, schema);
    ASSERT_TRUE(s.ok());
  }

  {
    docstore::DocumentStore store(this->test_dir_, s);
    ASSERT_TRUE(s.ok());

    nlohmann::json metadata;
    s = store.CheckCollectionInRegistry("users", metadata);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(metadata.contains("options"));
    ASSERT_TRUE(metadata.contains("schema"));
  }

  this->TearDown();
}

} // namespace docstore

int main(int argc, char **argv) { return leveldb::test::RunAllTests(); }