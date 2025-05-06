#include "docstore/document.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "leveldb/status.h"
#include "util/testharness.h"
#include <cassert>
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
  leveldb::Options options;

  options.primary_key = "user_id";
  options.secondary_key = "user_age";

  ASSERT_TRUE(s.ok());
  store.CreateCollection("users", options);
  ASSERT_TRUE(store.CheckCollectionInRegistry("users").ok());

  this->TearDown();
}

TEST(DocumentStoreTest, QueryOnNonExistentCollection) {
  leveldb::Status s;
  docstore::DocumentStore store(this->test_dir_, s);
  leveldb::Options options;
  ASSERT_TRUE(s.ok());
  s = store.LoadCollectionFromRegistry("users");
  ASSERT_TRUE(!s.ok());

  this->TearDown();
}

TEST(DocumentStoreTest, PutAndGetQuery) {
  leveldb::Status s;
  docstore::DocumentStore store(this->test_dir_, s);
  leveldb::Options options;
  ASSERT_TRUE(s.ok());
  nlohmann::json doc;
  doc["user_id"] = 1;
  doc["user_age"] = 30;
  doc["user_name"] = "harish";

  s = store.CreateCollection("users", options);
  ASSERT_TRUE(s.ok());
  store.Insert("users", "harish", "yayy");
  ASSERT_TRUE(s.ok());

  std::string value;
  s = store.Get("users", "harish", value);
  ASSERT_TRUE(s.ok());

  ASSERT_TRUE(value == "yayy");

  this->TearDown();
}

} // namespace docstore

int main(int argc, char **argv) { return leveldb::test::RunAllTests(); }