#include "docstore/document.h"
#include "leveldb/db.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "nlohmann/json_fwd.hpp"
#include "util/testharness.h"
#include <algorithm>
#include <atomic>
#include <cassert>
#include <iostream>
#include <nlohmann/json.hpp>
#include <thread>

class DocumentStoreTest {
public:
  std::string test_dir_ = "./test";
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

  auto parse_result = nlohmann::json::parse(value, nullptr, false);
  ASSERT_TRUE(!parse_result.is_discarded());
  nlohmann::json retrieved_doc = parse_result;
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

TEST(DocumentStoreTest, ConcurrentOperations) {
  leveldb::Status s;
  docstore::DocumentStore store(this->test_dir_, s);
  ASSERT_TRUE(s.ok());

  leveldb::Options options;
  options.primary_key = "user_id";
  nlohmann::json schema;
  schema = R"(
    {
      "fields": [
        {"user_id": "integer"},
        {"user_name": "string"}
      ],
      "required": ["user_id", "user_name"]
    }
  )"_json;

  s = store.CreateCollection("users", options, schema);
  ASSERT_TRUE(s.ok());

  std::vector<std::thread> put_threads;
  std::atomic<int> success_count{0};
  std::atomic<int> fail_count{0};

  for (int i = 0; i < 10; i++) {
    put_threads.emplace_back([&store, i, &success_count, &fail_count]() {
      nlohmann::json doc;
      doc["user_id"] = i;
      doc["user_name"] = "user_" + std::to_string(i);

      leveldb::Status s = store.Insert("users", doc);
      if (s.ok()) {
        success_count++;
      } else {
        fail_count++;
      }
    });
  }

  for (auto &thread : put_threads) {
    thread.join();
  }

  ASSERT_EQ(success_count, 10);
  ASSERT_EQ(fail_count, 0);

  std::vector<std::thread> get_threads;
  std::atomic<int> get_success_count{0};
  std::atomic<int> get_fail_count{0};

  for (int i = 0; i < 20; i++) {
    get_threads.emplace_back([&store, i, &get_success_count,
                              &get_fail_count]() {
      std::string value;
      int doc_id = i % 10;
      leveldb::Status s = store.Get("users", std::to_string(doc_id), value);

      if (s.ok() || s.IsNotFound()) {
        auto parse_result = nlohmann::json::parse(value, nullptr, false);
        if (!parse_result.is_discarded()) {
          nlohmann::json retrieved_doc = parse_result;
          if (retrieved_doc["user_name"] == "user_" + std::to_string(doc_id)) {
            get_success_count++;
          } else {
            get_fail_count++;
          }
        } else {
          get_fail_count++;
        }
      } else {
        get_fail_count++;
      }
    });
  }

  for (auto &thread : get_threads) {
    thread.join();
  }

  ASSERT_EQ(get_success_count, 20);
  ASSERT_EQ(get_fail_count, 0);

  this->TearDown();
}

TEST(DocumentStoreTest, GetAllDocuments) {
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
      "required": ["user_id", "user_age", "user_name"]
    }
  )"_json;

  s = store.CreateCollection("users", options, schema);
  ASSERT_TRUE(s.ok());

  // Insert 10 test documents
  for (int i = 1; i <= 10; ++i) {
    nlohmann::json doc;
    doc["user_id"] = i;
    doc["user_age"] = 20 + i;
    doc["user_name"] = "user_" + std::to_string(i);

    s = store.Insert("users", doc);
    ASSERT_TRUE(s.ok());
  }

  std::vector<nlohmann::json> documents;
  s = store.GetAll("users", documents);
  ASSERT_TRUE(s.ok());

  // Verify we got all 10 documents
  ASSERT_EQ(documents.size(), 10);

  std::vector<nlohmann::json> empty_docs;
  s = store.GetAll("non_existent", empty_docs);
  ASSERT_TRUE(!s.ok());
  ASSERT_TRUE(empty_docs.empty());

  this->TearDown();
}

TEST(DocumentStoreTest, SimulateSIGKILLAndRecovery) {
  leveldb::Status s;
  std::string test_dir = this->test_dir_ + "/sigkill_test";

  // First create and populate the database
  {
    docstore::DocumentStore store(test_dir, s);
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
        "required": ["user_id", "user_age", "user_name"]
      }
    )"_json;

    s = store.CreateCollection("users2", options, schema);
    ASSERT_TRUE(s.ok());

    // Insert some test data
    for (int i = 1; i <= 5; ++i) {
      nlohmann::json doc;
      doc["user_id"] = i;
      doc["user_age"] = 20 + i;
      doc["user_name"] = "user_" + std::to_string(i);

      s = store.Insert("users2", doc);
      ASSERT_TRUE(s.ok());
    }
  }

  {
    docstore::DocumentStore store(test_dir, s);
    ASSERT_TRUE(s.ok());

    std::vector<nlohmann::json> documents;
    s = store.GetAll("users2", documents);
    if (!s.ok()) {
      std::cout << "Error in GetAll: " << s.ToString() << std::endl;
    }

    ASSERT_TRUE(s.ok());
    ASSERT_EQ(documents.size(), 5);

    nlohmann::json new_doc;
    new_doc["user_id"] = 6;
    new_doc["user_age"] = 26;
    new_doc["user_name"] = "user_6";

    s = store.Insert("users2", new_doc);
    ASSERT_TRUE(s.ok());

    documents.clear();
    s = store.GetAll("users2", documents);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(documents.size(), 6);
  }

  this->TearDown();
}

TEST(DocumentStoreTest, ReopenAndGetSec) {
  leveldb::Status s;
  std::string test_dir = this->test_dir_ + "/reopen_test";

  // First create and populate the database
  {
    docstore::DocumentStore store(test_dir, s);
    ASSERT_TRUE(s.ok());

    leveldb::Options options;
    options.primary_key = "user_id";
    options.secondary_key = "user_age";
    options.filter_policy = leveldb::NewBloomFilterPolicy(20); //important for index recovery

    nlohmann::json schema;
    schema = R"(
      {
        "fields": [
          {"user_id": "integer"},
          {"user_age": "integer"},
          {"user_name": "string"}
        ],
        "required": ["user_id", "user_age", "user_name"]
      }
    )"_json;

    s = store.CreateCollection("users4", options, schema);
    ASSERT_TRUE(s.ok());

    // Insert test data with different ages
    /**
        1,2,3,4,5,6,7,8,9,10
        1, 2, 0, 1, 2, 0, 1, 2, 0, 1
        21,22,20,21,22,20,21,22,20,21
     */
    for (int i = 1; i <= 10; ++i) {
      nlohmann::json doc;
      doc["user_id"] = i;
      doc["user_age"] =
          20 + (i % 3);
      doc["user_name"] = "user_" + std::to_string(i);

      s = store.Insert("users4", doc);
      ASSERT_TRUE(s.ok());
    }
  }

  // Second reopen and get by age 22
  {
    docstore::DocumentStore store(test_dir, s);
    ASSERT_TRUE(s.ok());

    std::vector<nlohmann::json> documents;
    s = store.GetAll("users4", documents);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(documents.size(),
              10); // Should get users with age 22 (users 2, 5, 8)

    std::vector<leveldb::SecondayKeyReturnVal> values;
    s = store.GetSec("users4", "22", &values);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(values.size(), 3);
  }

  // Fourth reopen and get range of ages
  {
    docstore::DocumentStore store(test_dir, s);
    ASSERT_TRUE(s.ok());

    std::vector<leveldb::SecondayKeyReturnVal> values;
    s = store.RangeGetSec("users4", "21", "22", &values);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(values.size(),7); // Should get all users with ages 21 and 22
  }

  this->TearDown();
}

} // namespace docstore

int main(int argc, char **argv) { return leveldb::test::RunAllTests(); }