// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/coding.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile *file;
  Table *table;
};

static void DeleteEntry(const Slice &key, void *value) {
  TableAndFile *tf = reinterpret_cast<TableAndFile *>(value);
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void *arg1, void *arg2) {
  Cache *cache = reinterpret_cast<Cache *>(arg1);
  Cache::Handle *h = reinterpret_cast<Cache::Handle *>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string &dbname, const Options *options,
                       int entries)
    : env_(options->env), dbname_(dbname), options_(options),
      cache_(NewLRUCache(entries)) {
  if (!options->interval_tree_file_name.empty())
    intervalTree_ =
        new TwoDITwTopK(dbname + "/" + options->interval_tree_file_name, true);
  else
    intervalTree_ = NULL;
}

TableCache::~TableCache() { delete cache_; }

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle **handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == NULL) {
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile *file = NULL;
    Table *table = NULL;
    s = env_->NewRandomAccessFile(fname, &file);
    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname, &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = Table::Open(*options_, file, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile *tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
    }
  }
  return s;
}

Iterator *TableCache::NewIterator(const ReadOptions &options,
                                  uint64_t file_number, uint64_t file_size,
                                  Table **tableptr) {
  if (tableptr != NULL) {
    *tableptr = NULL;
  }

  Cache::Handle *handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table *table = reinterpret_cast<TableAndFile *>(cache_->Value(handle))->table;
  Iterator *result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions &options, uint64_t file_number,
                       uint64_t file_size, const Slice &k, void *arg,
                       bool (*saver)(void *, const Slice &, const Slice &)) {
  Cache::Handle *handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table *t = reinterpret_cast<TableAndFile *>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, saver);
    cache_->Release(handle);
  }
  return s;
}

Status TableCache::Get(const ReadOptions &options, uint64_t file_number,
                       uint64_t file_size, const Slice &k, void *arg,
                       bool (*saver)(void *, const Slice &, const Slice &,
                                     std::string &secKey, int &topKOutput,
                                     DBImpl *db),
                       string &secKey, int &topKOutput, DBImpl *db) {
  Cache::Handle *handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table *t = reinterpret_cast<TableAndFile *>(cache_->Value(handle))->table;
    if (this->options_->interval_tree_file_name.empty())
      s = t->InternalGetWithInterval(options, k, arg, saver, secKey, topKOutput,
                                     db);
    else
      s = t->InternalGet(options, k, arg, saver, secKey, topKOutput, db);

    cache_->Release(handle);
  } else {
    cout << "file not found!" << endl;
  }
  return s;
}

Status TableCache::Get(const ReadOptions &options, uint64_t file_number,
                       uint64_t file_size, const Slice &blockKey,
                       const Slice &k, void *arg,
                       bool (*saver)(void *, const Slice &, const Slice &,
                                     std::string &secKey, int &topKOutput,
                                     DBImpl *db),
                       string &secKey, int &topKOutput, DBImpl *db) {
  Cache::Handle *handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table *t = reinterpret_cast<TableAndFile *>(cache_->Value(handle))->table;

    s = t->InternalGet(options, blockKey, k, arg, saver, secKey, topKOutput,
                       db);
    cache_->Release(handle);
  } else {
    cout << "file not found!" << endl;
  }
  return s;
}

Status TableCache::RangeLookUp(const ReadOptions &options, uint64_t file_number,
                               uint64_t file_size, const Slice &startk,
                               const Slice &endk, void *arg,
                               bool (*saver)(void *, const Slice &,
                                             const Slice &, std::string &secKey,
                                             int &topKOutput, DBImpl *db),
                               string &secKey, int &topKOutput, DBImpl *db) {
  Cache::Handle *handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table *t = reinterpret_cast<TableAndFile *>(cache_->Value(handle))->table;
    // if(this->options_->IntervalTreeFileName.empty())
    s = t->RangeInternalGetWithInterval(options, startk, endk, arg, saver,
                                        secKey, topKOutput, db);

    cache_->Release(handle);
  } else {
    cout << "file not found!" << endl;
  }
  return s;
}

Status TableCache::RangeLookUp(const ReadOptions &options, uint64_t file_number,
                               uint64_t file_size, const Slice &blockKey,
                               void *arg,
                               bool (*saver)(void *, const Slice &,
                                             const Slice &, std::string &secKey,
                                             int &topKOutput, DBImpl *db),
                               string &secKey, int &topKOutput, DBImpl *db) {
  Cache::Handle *handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table *t = reinterpret_cast<TableAndFile *>(cache_->Value(handle))->table;
    s = t->RangeInternalGet(options, blockKey, arg, saver, secKey, topKOutput,
                            db);
    cache_->Release(handle);
  }
  return s;
}
void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

} // namespace leveldb
