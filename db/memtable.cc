// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "db_impl.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"
#include <nlohmann/json.hpp>
#include <sstream>
#include <unordered_set>

namespace leveldb {

static Slice GetLengthPrefixedSlice(const char *data) {
  uint32_t len;
  const char *p = data;
  p = GetVarint32Ptr(p, p + 5, &len); // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator &cmp, std::string secAtt)
    : comparator_(cmp), refs_(0), table_(comparator_, &arena_) {
  secAttribute = secAtt;
}

MemTable::MemTable(const InternalKeyComparator &cmp, const std::vector<std::string>& secAtts)
    : comparator_(cmp), refs_(0), table_(comparator_, &arena_), secondaryAttributes_(secAtts) {
  for (const auto& attr : secAtts) {
    multiSecTables_[attr] = SecMemTable();
  }
}

MemTable::~MemTable() {

  SecMemTable::const_iterator lookup = secTable_.begin();
  for (; lookup != secTable_.end(); lookup.increment()) {
    pair<string, vector<string> *> pr = *lookup;
    delete pr.second;
  }
  secTable_.clear();
  assert(refs_ == 0);
}

size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }

int MemTable::KeyComparator::operator()(const char *aptr,
                                        const char *bptr) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.
static const char *EncodeKey(std::string *scratch, const Slice &target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}

class MemTableIterator : public Iterator {
public:
  explicit MemTableIterator(MemTable::Table *table) : iter_(table) {}

  virtual bool Valid() const { return iter_.Valid(); }
  virtual void Seek(const Slice &k) { iter_.Seek(EncodeKey(&tmp_, k)); }
  virtual void SeekToFirst() { iter_.SeekToFirst(); }
  virtual void SeekToLast() { iter_.SeekToLast(); }
  virtual void Next() { iter_.Next(); }
  virtual void Prev() { iter_.Prev(); }
  virtual Slice key() const { return GetLengthPrefixedSlice(iter_.key()); }
  virtual Slice value() const {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  virtual Status status() const { return Status::OK(); }

private:
  MemTable::Table::Iterator iter_;
  std::string tmp_; // For passing to EncodeKey

  // No copying allowed
  MemTableIterator(const MemTableIterator &);
  void operator=(const MemTableIterator &);
};

Iterator *MemTable::NewIterator() { return new MemTableIterator(&table_); }

void MemTable::Add(SequenceNumber s, ValueType type, const Slice &key,
                   const Slice &value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len = VarintLength(internal_key_size) +
                             internal_key_size + VarintLength(val_size) +
                             val_size;
  char *buf = arena_.Allocate(encoded_len);
  char *p = EncodeVarint32(buf, internal_key_size);
  memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  memcpy(p, value.data(), val_size);
  assert((p + val_size) - buf == encoded_len);
  table_.Insert(buf);

  // Secondary MemTable logic
  if (type == kTypeDeletion)
    return;

  nlohmann::json doc;
  try {
    doc = nlohmann::json::parse(value.ToString());
  } catch (const nlohmann::json::parse_error &e) {
    return;
  }

  // NEW: Multi-secondary index support
  // Process all configured secondary keys
  for (const auto& secKeyName : secondaryAttributes_) {
    if (!doc.contains(secKeyName) || doc[secKeyName].is_null())
      continue;

    std::ostringstream skey;
    if (doc[secKeyName].is_number_unsigned()) {
      skey << doc[secKeyName].get<uint64_t>();
    } else if (doc[secKeyName].is_number_integer()) {
      skey << doc[secKeyName].get<int64_t>();
    } else if (doc[secKeyName].is_number_float()) {
      skey << doc[secKeyName].get<double>();
    } else if (doc[secKeyName].is_string()) {
      skey << doc[secKeyName].get<std::string>();
    } else if (doc[secKeyName].is_boolean()) {
      skey << (doc[secKeyName].get<bool>() ? "true" : "false");
    }

    std::string secKey = skey.str();
    
    // Use multiSecTables_ for multi-secondary index support
    auto& secTable = multiSecTables_[secKeyName];
    SecMemTable::const_iterator lookup = secTable.find(secKey);
    if (lookup == secTable.end()) {
      auto *invertedList = new std::vector<std::string>();
      invertedList->push_back(key.ToString());
      secTable.insert(std::make_pair(secKey, invertedList));
    } else {
      lookup->second->push_back(key.ToString());
    }
  }

  // LEGACY: Keep the old single secondary index logic for backward compatibility
  const std::string &sKeyAtt = secAttribute;
  if (!doc.contains(sKeyAtt) || doc[sKeyAtt].is_null())
    return;

  std::ostringstream skey;
  if (doc[sKeyAtt].is_number_unsigned()) {
    skey << doc[sKeyAtt].get<uint64_t>();
  } else if (doc[sKeyAtt].is_number_integer()) {
    skey << doc[sKeyAtt].get<int64_t>();
  } else if (doc[sKeyAtt].is_number_float()) {
    skey << doc[sKeyAtt].get<double>();
  } else if (doc[sKeyAtt].is_string()) {
    skey << doc[sKeyAtt].get<std::string>();
  } else if (doc[sKeyAtt].is_boolean()) {
    skey << (doc[sKeyAtt].get<bool>() ? "true" : "false");
  }

  std::string secKey = skey.str();
  SecMemTable::const_iterator lookup = secTable_.find(secKey);
  if (lookup == secTable_.end()) {
    auto *invertedList = new std::vector<std::string>();
    invertedList->push_back(key.ToString());
    secTable_.insert(std::make_pair(secKey, invertedList));
  } else {
    lookup->second->push_back(key.ToString());
  }
}

bool MemTable::Get(const LookupKey &key, std::string *value, Status *s) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char *entry = iter.key();
    uint32_t key_length;
    const char *key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
      case kTypeValue: {
        Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
        value->assign(v.data(), v.size());
        return true;
      }
      case kTypeDeletion:
        *s = Status::NotFound(Slice());
        return true;
      }
    }
  }
  return false;
}
// SECONDARY MEMTABLE
bool MemTable::Get(const LookupKey &key, std::string *value, Status *s,
                   uint64_t *tag) {
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char *entry = iter.key();
    uint32_t key_length;
    const char *key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key
      *tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(*tag & 0xff)) {
      case kTypeValue: {
        Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
        value->assign(v.data(), v.size());
        return true;
      }
      case kTypeDeletion:
        *s = Status::NotFound(Slice());
        return true;
      }
    }
  }
  return false;
}
// SECONDARY MEMTABLE
void MemTable::Get(const Slice &skey, SequenceNumber snapshot,
                   std::vector<SecondayKeyReturnVal> *value, Status *s,
                   std::unordered_set<std::string> *resultSetofKeysFound,
                   int topKOutput) {

  auto lookup = secTable_.find(skey.ToString());
  if (lookup != secTable_.end()) {

    pair<string, vector<string> *> pr = *lookup;
    for (int i = pr.second->size() - 1; i >= 0; i--) {
      if (value->size() >= topKOutput)
        return;

      Slice pkey = pr.second->at(i);
      LookupKey lkey(pkey, snapshot);
      std::string svalue;
      Status s;
      uint64_t tag;
      if (this->Get(lkey, &svalue, &s, &tag)) {
        if (!s.IsNotFound()) {
          nlohmann::json doc;

          try {
            doc = nlohmann::json::parse(svalue);
          } catch (const nlohmann::json::parse_error &e) {
            continue;
          }

          const std::string &sKeyAtt = secAttribute;

          if (!doc.contains(sKeyAtt) || doc[sKeyAtt].is_null())
            continue;

          std::ostringstream tempskey;
          if (doc[sKeyAtt].is_number_unsigned()) {
            tempskey << doc[sKeyAtt].get<uint64_t>();
          } else if (doc[sKeyAtt].is_number_integer()) {
            tempskey << doc[sKeyAtt].get<int64_t>();
          } else if (doc[sKeyAtt].is_number_float()) {
            tempskey << doc[sKeyAtt].get<double>();
          } else if (doc[sKeyAtt].is_string()) {
            tempskey << doc[sKeyAtt].get<std::string>();
          } else if (doc[sKeyAtt].is_boolean()) {
            tempskey << (doc[sKeyAtt].get<bool>() ? "true" : "false");
          }

          Slice valsKey = tempskey.str();
          if (comparator_.comparator.user_comparator()->Compare(valsKey,
                                                                skey) == 0) {
            struct SecondayKeyReturnVal newVal;
            newVal.key = pr.second->at(i);

            if (resultSetofKeysFound->find(newVal.key) ==
                resultSetofKeysFound->end()) {
              newVal.value = svalue;
              newVal.sequence_number = tag;

              if (value->size() < topKOutput) {
                newVal.Push(value, newVal);
                resultSetofKeysFound->insert(newVal.key);
              } else if (newVal.sequence_number >
                         value->front().sequence_number) {
                newVal.Pop(value);
                newVal.Push(value, newVal);
                resultSetofKeysFound->insert(newVal.key);
                resultSetofKeysFound->erase(
                    resultSetofKeysFound->find(value->front().key));
              }
            }
          }
        }
      }
    }
  }
}
void MemTable::RangeLookUp(
    const Slice &startSkey, const Slice &endSkey, SequenceNumber snapshot,
    std::vector<SecondayKeyReturnVal> *value, Status *s,
    std::unordered_set<std::string> *resultSetofKeysFound, int topKOutput) {

  auto lookuplb = secTable_.lower_bound(startSkey.ToString());
  auto lookupub = secTable_.upper_bound(endSkey.ToString());
  for (; lookuplb != lookupub; lookuplb++) {

    pair<string, vector<string> *> pr = *lookuplb;
    for (int i = pr.second->size() - 1; i >= 0; i--) {
      if (value->size() >= topKOutput)
        continue;

      Slice pkey = pr.second->at(i);
      LookupKey lkey(pkey, snapshot);
      std::string svalue;
      Status s;
      uint64_t tag;
      if (this->Get(lkey, &svalue, &s, &tag)) {
        if (!s.IsNotFound()) {
          nlohmann::json doc;

          try {
            doc = nlohmann::json::parse(svalue);
          } catch (const nlohmann::json::parse_error &e) {
            continue;
          }

          const std::string &sKeyAtt = secAttribute;

          // Check if the secondary key attribute exists and is not null
          if (!doc.contains(sKeyAtt) || doc[sKeyAtt].is_null())
            continue;

          std::ostringstream tempskey;
          if (doc[sKeyAtt].is_number_unsigned()) {
            tempskey << doc[sKeyAtt].get<uint64_t>();
          } else if (doc[sKeyAtt].is_number_integer()) {
            tempskey << doc[sKeyAtt].get<int64_t>();
          } else if (doc[sKeyAtt].is_number_float()) {
            tempskey << doc[sKeyAtt].get<double>();
          } else if (doc[sKeyAtt].is_string()) {
            tempskey << doc[sKeyAtt].get<std::string>();
          } else if (doc[sKeyAtt].is_boolean()) {
            tempskey << (doc[sKeyAtt].get<bool>() ? "true" : "false");
          }

          Slice valsKey = tempskey.str();
          if (comparator_.comparator.user_comparator()->Compare(
                  valsKey, pr.first) == 0) {
            struct SecondayKeyReturnVal newVal;
            newVal.key = pr.second->at(i);
            std::string temp;

            if (resultSetofKeysFound->find(newVal.key) ==
                resultSetofKeysFound->end()) {

              newVal.value = svalue;
              newVal.sequence_number = tag;

              if (value->size() < topKOutput) {

                newVal.Push(value, newVal);
                resultSetofKeysFound->insert(newVal.key);

              } else if (newVal.sequence_number >
                         value->front().sequence_number) {

                newVal.Pop(value);
                newVal.Push(value, newVal);
                resultSetofKeysFound->insert(newVal.key);
                resultSetofKeysFound->erase(
                    resultSetofKeysFound->find(value->front().key));
              }
            }
          }
        }
      }
    }
  }
}

// NEW: GetBySecondaryKey implementation for specific secondary index attributes
void MemTable::GetBySecondaryKey(const std::string& attribute, const Slice &skey, 
                                 SequenceNumber snapshot,
                                 std::vector<SecondayKeyReturnVal> *value, Status *s,
                                 std::unordered_set<std::string> *resultSetofKeysFound,
                                 int topKOutput) {
  
  // Check if this attribute exists in our multi-secondary index
  auto sec_table_it = multiSecTables_.find(attribute);
  if (sec_table_it == multiSecTables_.end()) {
    //Just return if the attribute does not exist in the multi-secondary index
    return;
  }

  auto& secTable = sec_table_it->second;
  auto lookup = secTable.find(skey.ToString());
  if (lookup != secTable.end()) {

    pair<string, vector<string> *> pr = *lookup;
    for (int i = pr.second->size() - 1; i >= 0; i--) {
      if (value->size() >= topKOutput)
        return;

      Slice pkey = pr.second->at(i);
      LookupKey lkey(pkey, snapshot);
      std::string svalue;
      Status s;
      uint64_t tag;
      if (this->Get(lkey, &svalue, &s, &tag)) {
        if (!s.IsNotFound()) {
          nlohmann::json doc;

          try {
            doc = nlohmann::json::parse(svalue);
          } catch (const nlohmann::json::parse_error &e) {
            continue;
          }

          if (!doc.contains(attribute) || doc[attribute].is_null())
            continue;

          std::ostringstream tempskey;
          if (doc[attribute].is_number_unsigned()) {
            tempskey << doc[attribute].get<uint64_t>();
          } else if (doc[attribute].is_number_integer()) {
            tempskey << doc[attribute].get<int64_t>();
          } else if (doc[attribute].is_number_float()) {
            tempskey << doc[attribute].get<double>();
          } else if (doc[attribute].is_string()) {
            tempskey << doc[attribute].get<std::string>();
          } else if (doc[attribute].is_boolean()) {
            tempskey << (doc[attribute].get<bool>() ? "true" : "false");
          }

          Slice valsKey = tempskey.str();
          if (comparator_.comparator.user_comparator()->Compare(valsKey, skey) == 0) {
            struct SecondayKeyReturnVal newVal;
            newVal.key = pr.second->at(i);

            if (resultSetofKeysFound->find(newVal.key) == resultSetofKeysFound->end()) {
              newVal.value = svalue;
              newVal.sequence_number = tag;

              if (value->size() < topKOutput) {
                newVal.Push(value, newVal);
                resultSetofKeysFound->insert(newVal.key);
              } else if (newVal.sequence_number > value->front().sequence_number) {
                newVal.Pop(value);
                newVal.Push(value, newVal);
                resultSetofKeysFound->insert(newVal.key);
                resultSetofKeysFound->erase(resultSetofKeysFound->find(value->front().key));
              }
            }
          }
        }
      }
    }
  }
}

// NEW: RangeLookUpBySecondaryKey implementation for specific secondary index attributes
void MemTable::RangeLookUpBySecondaryKey(const std::string& attribute, 
                                         const Slice &startSkey, const Slice &endSkey,
                                         SequenceNumber snapshot,
                                         std::vector<SecondayKeyReturnVal> *value, Status *s,
                                         std::unordered_set<std::string> *resultSetofKeysFound,
                                         int topKOutput) {
  
  // Check if this attribute exists in our multi-secondary index
  auto sec_table_it = multiSecTables_.find(attribute);
  if (sec_table_it == multiSecTables_.end()) {
    // Fall back to the default secondary attribute if the requested one doesn't exist
    if (attribute == secAttribute) {
      RangeLookUp(startSkey, endSkey, snapshot, value, s, resultSetofKeysFound, topKOutput);
    }
    return;
  }

  auto& secTable = sec_table_it->second;
  auto lookuplb = secTable.lower_bound(startSkey.ToString());
  auto lookupub = secTable.upper_bound(endSkey.ToString());
  
  for (; lookuplb != lookupub; lookuplb++) {
    pair<string, vector<string> *> pr = *lookuplb;
    for (int i = pr.second->size() - 1; i >= 0; i--) {
      if (value->size() >= topKOutput)
        continue;

      Slice pkey = pr.second->at(i);
      LookupKey lkey(pkey, snapshot);
      std::string svalue;
      Status s;
      uint64_t tag;
      if (this->Get(lkey, &svalue, &s, &tag)) {
        if (!s.IsNotFound()) {
          nlohmann::json doc;

          try {
            doc = nlohmann::json::parse(svalue);
          } catch (const nlohmann::json::parse_error &e) {
            continue;
          }

          if (!doc.contains(attribute) || doc[attribute].is_null())
            continue;

          std::ostringstream tempskey;
          if (doc[attribute].is_number_unsigned()) {
            tempskey << doc[attribute].get<uint64_t>();
          } else if (doc[attribute].is_number_integer()) {
            tempskey << doc[attribute].get<int64_t>();
          } else if (doc[attribute].is_number_float()) {
            tempskey << doc[attribute].get<double>();
          } else if (doc[attribute].is_string()) {
            tempskey << doc[attribute].get<std::string>();
          } else if (doc[attribute].is_boolean()) {
            tempskey << (doc[attribute].get<bool>() ? "true" : "false");
          }

          Slice valsKey = tempskey.str();
          if (comparator_.comparator.user_comparator()->Compare(valsKey, pr.first) == 0) {
            struct SecondayKeyReturnVal newVal;
            newVal.key = pr.second->at(i);

            if (resultSetofKeysFound->find(newVal.key) == resultSetofKeysFound->end()) {
              newVal.value = svalue;
              newVal.sequence_number = tag;

              if (value->size() < topKOutput) {
                newVal.Push(value, newVal);
                resultSetofKeysFound->insert(newVal.key);
              } else if (newVal.sequence_number > value->front().sequence_number) {
                newVal.Pop(value);
                newVal.Push(value, newVal);
                resultSetofKeysFound->insert(newVal.key);
                resultSetofKeysFound->erase(resultSetofKeysFound->find(value->front().key));
              }
            }
          }
        }
      }
    }
  }
}

} // namespace leveldb
