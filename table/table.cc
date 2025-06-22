// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"
#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "rapidjson/document.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include <fstream>
#include <sstream>
#include <unordered_set>

namespace leveldb {

IOStat pr_iostat;
IOStat sr_iostat;
IOStat sr_range_iostat;
IOStat w_iostat;

std::vector<std::string> &split(const std::string &s, char delim,
                                std::vector<std::string> &elems) {
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, delim)) {
    elems.push_back(item);
  }
  return elems;
}

std::vector<std::string> split(const std::string &s, char delim) {
  std::vector<std::string> elems;
  split(s, delim, elems);
  return elems;
}
static Slice GetLengthPrefixedSlice(const char *data) {
  uint32_t len;
  const char *p = data;
  p = GetVarint32Ptr(p, p + 5, &len); // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
    delete interval_block;
    delete secondary_filter;
    delete[] secondary_filter_data;
    
    // NEW: Clean up multi-secondary filters
    for (auto& [key, filter] : multi_secondary_filters) {
      delete filter;
    }
    for (auto& [key, data] : multi_secondary_filter_data) {
      delete[] data;
    }
  }

  Options options;
  Status status;
  RandomAccessFile *file;
  uint64_t cache_id;
  FilterBlockReader *filter;
  const char *filter_data;

  FilterBlockReader *secondary_filter;
  const char *secondary_filter_data;

  // NEW: Multi-secondary filters support
  std::unordered_map<std::string, FilterBlockReader*> multi_secondary_filters;
  std::unordered_map<std::string, const char*> multi_secondary_filter_data;

  BlockHandle metaindex_handle; // Handle to metaindex_block: saved from footer
  Block *index_block;
  Block *interval_block;
};

Status Table::Open(const Options &options, RandomAccessFile *file,
                   uint64_t size, Table **table) {

  bool isinterval = options.interval_tree_file_name.empty();
  int footerlength;
  if (isinterval)
    footerlength = Footer::kEncodedLength + BlockHandle::kMaxEncodedLength;
  else
    footerlength = Footer::kEncodedLength;

  *table = NULL;

  if (size < footerlength) {
    return Status::InvalidArgument("file is too short to be an sstable");
  }

  char footer_space[footerlength];
  Slice footer_input;
  Status s = file->Read(size - footerlength, footerlength, &footer_input,
                        footer_space);
  if (!s.ok())
    return s;

  Footer footer;

  s = footer.DecodeFrom(&footer_input, isinterval);
  if (!s.ok())
    return s;

  ReadOptions roptions;
  roptions.type = ReadType::Meta;

  // Read the index block
  BlockContents contents;
  Block *index_block = NULL;
  if (s.ok()) {
    s = ReadBlock(file, roptions, footer.index_handle(), &contents);
    if (s.ok()) {
      index_block = new Block(contents);
    }
  }

  // Read the interval block
  Block *interval_block = NULL;

  if (isinterval) {
    BlockContents contents;

    if (s.ok()) {
      s = ReadBlock(file, roptions, footer.interval_handle(), &contents);
      if (s.ok()) {
        interval_block = new Block(contents);
      }
    }
  }

  Rep *rep = new Table::Rep;

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.

    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->interval_block = interval_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = NULL;
    rep->filter = NULL;
    rep->secondary_filter_data = NULL;
    rep->secondary_filter = NULL;
    *table = new Table(rep);
    (*table)->ReadMeta(footer);
  } else {
    if (index_block)
      delete index_block;
    if (interval_block)
      delete interval_block;
  }

  return s;
}

void Table::ReadMeta(const Footer &footer) {

  // ofstream outputFile;
  // outputFile.open("/Users/nakshikatha/Desktop/test
  // codes/debug.txt",std::ofstream::out | std::ofstream::app);
  // outputFile<<"read meta\n";
  if (rep_->options.filter_policy == NULL) {
    return; // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  opt.type = ReadType::Meta;
  BlockContents contents;
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }
  Block *meta = new Block(contents);

  Iterator *iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    // outputFile<<"valid P\n";
    ReadFilter(iter->value());
  }

  std::string skey = "secondaryfilter.";
  skey.append(rep_->options.filter_policy->Name());
  iter->Seek(skey);
  // outputFile<<(iter->Valid())<<endl;
  if (iter->Valid() && iter->key() == Slice(skey)) {
    // outputFile<<"valid S\n";
    ReadSecondaryFilter(iter->value());
    // outputFile<<(rep_->secondary_filter==NULL)<<endl;
  }

  // NEW: Read multiple secondary filters for different attributes
  for (const auto& sec_key : rep_->options.secondary_keys) {
    std::string multi_skey = "secondaryfilter." + sec_key;
    iter->Seek(multi_skey);
    if (iter->Valid() && iter->key() == Slice(multi_skey)) {
      ReadMultiSecondaryFilter(iter->value(), sec_key);
    }
  }

  delete iter;
  delete meta;
}

void Table::ReadFilter(const Slice &filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  opt.type = ReadType::Meta;
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data(); // Will need to delete later
  }
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

void Table::ReadSecondaryFilter(const Slice &filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  opt.type = ReadType::Meta;
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->secondary_filter_data =
        block.data.data(); // Will need to delete later
  }
  rep_->secondary_filter =
      new FilterBlockReader(rep_->options.filter_policy, block.data);
}

// NEW: Read multi-secondary filter for a specific attribute
void Table::ReadMultiSecondaryFilter(const Slice &filter_handle_value, const std::string &attribute) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  opt.type = ReadType::Meta;
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  if (block.heap_allocated) {
    rep_->multi_secondary_filter_data[attribute] = block.data.data(); // Will need to delete later
  }
  rep_->multi_secondary_filters[attribute] = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

static void DeleteBlock(void *arg, void *ignored) {
  delete reinterpret_cast<Block *>(arg);
}

static void DeleteCachedBlock(const Slice &key, void *value) {
  Block *block = reinterpret_cast<Block *>(value);
  delete block;
}

static void ReleaseBlock(void *arg, void *h) {
  Cache *cache = reinterpret_cast<Cache *>(arg);
  Cache::Handle *handle = reinterpret_cast<Cache::Handle *>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
Iterator *Table::BlockReader(void *arg, const ReadOptions &options,
                             const Slice &index_value) {
  Table *table = reinterpret_cast<Table *>(arg);
  Cache *block_cache = table->rep_->options.block_cache;
  Block *block = NULL;
  Cache::Handle *cache_handle = NULL;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    if (options.type == ReadType::Write)
      w_iostat.numberofIO++;
    else if (options.type == ReadType::PRead)
      pr_iostat.numberofIO++;
    else if (options.type == ReadType::SRead)
      sr_iostat.numberofIO++;
    else if (options.type == ReadType::SRRead)
      sr_range_iostat.numberofIO++;

    BlockContents contents;
    if (block_cache != NULL) {
      char cache_key_buffer[16];
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      cache_handle = block_cache->Lookup(key);
      if (cache_handle != NULL) {
        block = reinterpret_cast<Block *>(block_cache->Value(cache_handle));

        if (options.type == ReadType::Write)
          w_iostat.cachehit++;
        else if (options.type == ReadType::PRead)
          pr_iostat.cachehit++;
        else if (options.type == ReadType::SRead)
          sr_iostat.cachehit++;
        else if (options.type == ReadType::SRRead)
          sr_range_iostat.cachehit++;

      } else {
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          if (contents.cachable && options.fill_cache) {
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator *iter;
  if (block != NULL) {
    iter = block->NewIterator(table->rep_->options.comparator);
    if (cache_handle == NULL) {
      iter->RegisterCleanup(&DeleteBlock, block, NULL);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    iter = NewErrorIterator(s);
  }
  return iter;
}

Iterator *Table::NewIterator(const ReadOptions &options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table *>(this), options);
}

Status Table::InternalGet(const ReadOptions &options, const Slice &k, void *arg,
                          bool (*saver)(void *, const Slice &, const Slice &)) {
  Status s;
  Iterator *iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  if (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader *filter = rep_->filter;
    BlockHandle handle;
    if (filter != NULL && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found

      pr_iostat.prunebloomfilter++;

    } else {
      // leveldb::iostat.numberofIO++;
      Iterator *block_iter = BlockReader(this, options, iiter->value());
      block_iter->Seek(k);
      if (block_iter->Valid()) {

        bool f = (*saver)(arg, block_iter->key(), block_iter->value());

        if (f == false) {
          pr_iostat.bloomfilterFP++;
        }
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

Status Table::InternalGet(const ReadOptions &options, const Slice &k, void *arg,
                          bool (*saver)(void *, const Slice &, const Slice &,
                                        std::string &secKey, int &topKOutput,
                                        DBImpl *db),
                          string &secKey, int &topKOutput, DBImpl *db) {
  // ofstream outputFile;
  // outputFile.open("/Users/nakshikatha/Desktop/test codes/debug.txt");
  // outputFile<<k.ToString()<<"\n\nStart:\n\n";
  Status s;
  Iterator *iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->SeekToFirst();

  // outputFile<<"in2\n";
  int p = 1;
  while (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader *filter = rep_->secondary_filter;
    BlockHandle handle;
    // outputFile<<(filter !=
    // NULL)<<endl;//(!filter->KeyMayMatch(handle.offset(),
    // k));//(handle.DecodeFrom(&handle_value).ok());
    if (filter != NULL && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // outputFile<<(!filter->KeyMayMatch(handle.offset(), k))<<"\n";
      // outputFile<<"false\n";
      // Not found

      sr_iostat.prunebloomfilter++;

    } else {

      // outputFile<<"true\n";
      // leveldb::iostat.numberofIO++;
      Iterator *block_iter = BlockReader(this, options, iiter->value());
      block_iter->SeekToFirst();
      while (block_iter->Valid()) {

        bool f = (*saver)(arg, block_iter->key(), block_iter->value(), secKey,
                          topKOutput, db);
        if (f == false) {
          sr_iostat.bloomfilterFP++;
        }
        // if(f)
        // outputFile<<"saved\n";
        // outputFile<<newVal.key.ToString()<<endl<<newVal.value.ToString()<<endl;

        /*  if(f)
               kNoOfOutputs--;



           if(kNoOfOutputs<=0)
           {
               if (s.ok()) {
                   s = iiter->status();
                 }

               //delete block_iter;
               //delete iiter;
               //outputFile.close();
               return s;
           }


      */

        block_iter->Next();
      }
      s = block_iter->status();
      delete block_iter;
    }

    iiter->Next();
  }

  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  // outputFile.close();
  return s;
}

Status Table::InternalGetWithInterval(
    const ReadOptions &options, const Slice &k, void *arg,
    bool (*saver)(void *, const Slice &, const Slice &, std::string &secKey,
                  int &topKOutput, DBImpl *db),
    string &secKey, int &topKOutput, DBImpl *db) {

  Status s;
  Iterator *iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->SeekToFirst();

  Iterator *iterInterval =
      rep_->interval_block->NewIterator(rep_->options.comparator);
  iterInterval->SeekToFirst();

  // std::string keystr = k.ToString().substr(0, k.size()-8);
  // cout<<keystr<<endl;
  const char *ks = k.data();
  Slice sk = Slice(ks, k.size() - 8);

  while (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader *filter = rep_->secondary_filter;
    BlockHandle handle;
    // std::vector<std::string> x ;
    Slice key, value;
    if (iterInterval->Valid()) {
      key = iterInterval->key();
      value = iterInterval->value();
      // x = split(iterInterval->value().ToString(), ',');
      // cout<<key<<" to "<<value<<endl;
    }
    // outputFile<<(filter !=
    // NULL)<<endl;//(!filter->KeyMayMatch(handle.offset(),
    // k));//(handle.DecodeFrom(&handle_value).ok());
    if (sk.compare(key) < 0 || sk.compare(value) > 0) {
      // leveldb::iostat.pruneinterval++;
      sr_iostat.pruneinterval++;
      // cout<<"Prune Interval\n"<< leveldb::iostat.pruneinterval;
    } else if (filter != NULL && handle.DecodeFrom(&handle_value).ok() &&
               !filter->KeyMayMatch(handle.offset(), k)) {
      sr_iostat.prunebloomfilter++;

      // outputFile<<(!filter->KeyMayMatch(handle.offset(), k))<<"\n";
      // outputFile<<"false\n";
      // Not found
      // cout<<"BloomFilter Interval\n";
    }
    //      else if(x.size()==3 && (keystr.compare( x[0])<0 ||
    //      keystr.compare(x[1]) >0 ))
    //      {
    //    	  cout<<"Prune Interval\n";
    //      }
    //		else if(sk.compare(key)<0 || sk.compare(value) >0 )
    //		{
    //		  cout<<"Prune Interval\n";
    //		}
    else {

      // outputFile<<"true\n";

      Iterator *block_iter = BlockReader(this, options, iiter->value());
      // leveldb::iostat.numberofIO++;
      block_iter->SeekToFirst();
      while (block_iter->Valid()) {

        bool f = (*saver)(arg, block_iter->key(), block_iter->value(), secKey,
                          topKOutput, db);
        if (f == false)
          sr_iostat.bloomfilterFP++;
        block_iter->Next();
      }

      s = block_iter->status();
      delete block_iter;
    }

    iiter->Next();
    iterInterval->Next();
  }

  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  delete iterInterval;
  // outputFile.close();
  return s;
}

Status Table::RangeInternalGetWithInterval(
    const ReadOptions &options, const Slice &startk, const Slice &endk,
    void *arg,
    bool (*saver)(void *, const Slice &, const Slice &, std::string &secKey,
                  int &topKOutput, DBImpl *db),
    string &secKey, int &topKOutput, DBImpl *db) {

  Status s;
  Iterator *iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->SeekToFirst();

  Iterator *iterInterval =
      rep_->interval_block->NewIterator(rep_->options.comparator);
  iterInterval->SeekToFirst();

  // std::string keystr = k.ToString().substr(0, k.size()-8);
  // cout<<keystr<<endl;
  //	const char* ks = k.data();
  //	Slice sk = Slice(ks, k.size()-8);

  while (iiter->Valid()) {
    Slice handle_value = iiter->value();
    FilterBlockReader *filter = rep_->secondary_filter;
    BlockHandle handle;
    // std::vector<std::string> x ;
    Slice key, value;
    if (iterInterval->Valid()) {
      key = iterInterval->key();
      value = iterInterval->value();
    }
    // outputFile<<(filter !=
    // NULL)<<endl;//(!filter->KeyMayMatch(handle.offset(),
    // k));//(handle.DecodeFrom(&handle_value).ok());
    if (startk.compare(value) > 0 || endk.compare(key) < 0) {
      // cout<<"Prune Interval\n";
      sr_range_iostat.pruneinterval++;
    }

    else {

      // outputFile<<"true\n";

      Iterator *block_iter = BlockReader(this, options, iiter->value());
      // leveldb::iostat.numberofIO++;
      block_iter->SeekToFirst();
      while (block_iter->Valid()) {

        bool f = (*saver)(arg, block_iter->key(), block_iter->value(), secKey,
                          topKOutput, db);
        block_iter->Next();
      }
      s = block_iter->status();
      delete block_iter;
    }

    iiter->Next();
    iterInterval->Next();
  }

  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  delete iterInterval;
  // outputFile.close();
  return s;
}

Status Table::RangeInternalGet(const ReadOptions &options, const Slice &k,
                               void *arg,
                               bool (*saver)(void *, const Slice &,
                                             const Slice &, std::string &secKey,
                                             int &topKOutput, DBImpl *db),
                               string &secKey, int &topKOutput, DBImpl *db) {

  Status s;
  Iterator *iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);

  if (iiter->Valid()) {
    // Slice handle_value = iiter->value();

    BlockHandle handle;

    Iterator *block_iter = BlockReader(this, options, iiter->value());
    block_iter->SeekToFirst();
    while (block_iter->Valid()) {
      // outputFile<<"in\n";
      bool f = (*saver)(arg, block_iter->key(), block_iter->value(), secKey,
                        topKOutput, db);
      block_iter->Next();
    }
    s = block_iter->status();
    delete block_iter;
    //}

    if (s.ok()) {
      s = iiter->status();
    }

  } else
    s.IOError("");

  delete iiter;
  // outputFile.close();
  return s;
}

Status Table::InternalGet(const ReadOptions &options, const Slice &blockkey,
                          const Slice &pointkey, void *arg,
                          bool (*saver)(void *, const Slice &, const Slice &,
                                        std::string &secKey, int &topKOutput,
                                        DBImpl *db),
                          string &secKey, int &topKOutput, DBImpl *db) {

  Status s;
  Iterator *iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(blockkey);

  if (iiter->Valid()) {

    Slice handle_value = iiter->value();
    FilterBlockReader *filter = rep_->secondary_filter;
    BlockHandle handle;
    // outputFile<<(filter !=
    // NULL)<<endl;//(!filter->KeyMayMatch(handle.offset(),
    // k));//(handle.DecodeFrom(&handle_value).ok());
    if (filter != NULL && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), pointkey)) {

    }
    // Slice handle_value = iiter->value();

    // BlockHandle handle;
    else {
      Iterator *block_iter = BlockReader(this, options, iiter->value());
      block_iter->SeekToFirst();
      while (block_iter->Valid()) {
        bool f = (*saver)(arg, block_iter->key(), block_iter->value(), secKey,
                          topKOutput, db);
        block_iter->Next();
      }
      s = block_iter->status();
      delete block_iter;
    }
    //}

    if (s.ok()) {
      s = iiter->status();
    }

  } else
    s.IOError("");

  delete iiter;
  // outputFile.close();
  return s;
}

uint64_t Table::ApproximateOffsetOf(const Slice &key) const {
  Iterator *index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

// NEW: InternalGet method with attribute parameter for multi-secondary index support
Status Table::InternalGet(const ReadOptions &options, const Slice &k, void *arg,
                          bool (*saver)(void *, const Slice &, const Slice &,
                                        std::string &secKey, int &topKOutput,
                                        DBImpl *db),
                          const std::string &attribute, int &topKOutput, DBImpl *db) {
  Status s;
  Iterator *iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->SeekToFirst();

  // Get the filter for the specific attribute
  FilterBlockReader *filter = nullptr;
  auto filter_it = rep_->multi_secondary_filters.find(attribute);
  if (filter_it != rep_->multi_secondary_filters.end()) {
    filter = filter_it->second;
  } else {
    // Fall back to the default secondary filter if the attribute doesn't exist
    filter = rep_->secondary_filter;
  }

  while (iiter->Valid()) {
    Slice handle_value = iiter->value();
    BlockHandle handle;
    
    if (filter != NULL && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found
      sr_iostat.prunebloomfilter++;
    } else {
      Iterator *block_iter = BlockReader(this, options, iiter->value());
      block_iter->SeekToFirst();
      while (block_iter->Valid()) {
        std::string temp_secKey = attribute;  // Create temporary non-const string
        bool f = (*saver)(arg, block_iter->key(), block_iter->value(), temp_secKey,
                          topKOutput, db);
        if (f == false) {
          sr_iostat.bloomfilterFP++;
        }
        block_iter->Next();
      }
      s = block_iter->status();
      delete block_iter;
    }

    iiter->Next();
  }

  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

// NEW: InternalGetWithInterval method with attribute parameter for multi-secondary index support
Status Table::InternalGetWithInterval(const ReadOptions &options, const Slice &k, void *arg,
                                      bool (*saver)(void *, const Slice &, const Slice &,
                                                    std::string &secKey, int &topKOutput,
                                                    DBImpl *db),
                                      const std::string &attribute, int &topKOutput, DBImpl *db) {
  Status s;
  Iterator *iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->SeekToFirst();

  Iterator *iterInterval = rep_->interval_block->NewIterator(rep_->options.comparator);
  iterInterval->SeekToFirst();

  const char *ks = k.data();
  Slice sk = Slice(ks, k.size() - 8);

  // Get the filter for the specific attribute
  FilterBlockReader *filter = nullptr;
  auto filter_it = rep_->multi_secondary_filters.find(attribute);
  if (filter_it != rep_->multi_secondary_filters.end()) {
    filter = filter_it->second;
  } else {
    // Fall back to the default secondary filter if the attribute doesn't exist
    filter = rep_->secondary_filter;
  }

  while (iiter->Valid()) {
    Slice handle_value = iiter->value();
    BlockHandle handle;
    Slice key, value;
    
    if (iterInterval->Valid()) {
      key = iterInterval->key();
      value = iterInterval->value();
    }
    
    if (sk.compare(key) < 0 || sk.compare(value) > 0) {
      sr_iostat.pruneinterval++;
    } else if (filter != NULL && handle.DecodeFrom(&handle_value).ok() &&
               !filter->KeyMayMatch(handle.offset(), k)) {
      sr_iostat.prunebloomfilter++;
    } else {
      Iterator *block_iter = BlockReader(this, options, iiter->value());
      block_iter->SeekToFirst();
      while (block_iter->Valid()) {
        std::string temp_secKey = attribute;  // Create temporary non-const string
        bool f = (*saver)(arg, block_iter->key(), block_iter->value(), temp_secKey,
                          topKOutput, db);
        if (f == false)
          sr_iostat.bloomfilterFP++;
        block_iter->Next();
      }
      s = block_iter->status();
      delete block_iter;
    }

    iiter->Next();
    iterInterval->Next();
  }

  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  delete iterInterval;
  return s;
}

// NEW: RangeInternalGetWithInterval method with attribute parameter for multi-secondary index support
Status Table::RangeInternalGetWithInterval(const ReadOptions &options, const Slice &startk, const Slice &endk,
                                           void *arg,
                                           bool (*saver)(void *, const Slice &, const Slice &,
                                                         std::string &secKey, int &topKOutput,
                                                         DBImpl *db),
                                           const std::string &attribute, int &topKOutput, DBImpl *db) {
  Status s;
  Iterator *iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->SeekToFirst();

  Iterator *iterInterval = rep_->interval_block->NewIterator(rep_->options.comparator);
  iterInterval->SeekToFirst();

  while (iiter->Valid()) {
    Slice handle_value = iiter->value();
    BlockHandle handle;
    Slice key, value;
    
    if (iterInterval->Valid()) {
      key = iterInterval->key();
      value = iterInterval->value();
    }
    
    if (startk.compare(value) > 0 || endk.compare(key) < 0) {
      sr_range_iostat.pruneinterval++;
    } else {
      Iterator *block_iter = BlockReader(this, options, iiter->value());
      block_iter->SeekToFirst();
      while (block_iter->Valid()) {
        std::string temp_secKey = attribute;  // Create temporary non-const string
        bool f = (*saver)(arg, block_iter->key(), block_iter->value(), temp_secKey,
                          topKOutput, db);
        block_iter->Next();
      }
      s = block_iter->status();
      delete block_iter;
    }

    iiter->Next();
    iterInterval->Next();
  }

  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  delete iterInterval;
  return s;
}

} // namespace leveldb
