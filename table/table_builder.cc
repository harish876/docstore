// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "rapidjson/document.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include <assert.h>
#include <fstream>
#include <sstream>
#include <unordered_map>

template <typename T> std::string ToString(const T &value) {
  std::ostringstream oss;
  oss << std::dec << value;
  return oss.str();
}
namespace leveldb {

struct TableBuilder::Rep {
  Options options;
  Options index_block_options;
  Options interval_block_options;
  WritableFile *file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  BlockBuilder interval_block;
  std::string last_key;
  int64_t num_entries;
  bool closed; // Either Finish() or Abandon() has been called.
  FilterBlockBuilder *filter_block;
  FilterBlockBuilder *secondary_filter_block;
  
  // NEW: Multiple secondary indexes support
  std::unordered_map<std::string, FilterBlockBuilder*> multi_secondary_filter_blocks_;
  std::unordered_map<std::string, BlockBuilder*> multi_interval_blocks_;
  std::unordered_map<std::string, std::string> multi_min_sec_values_;
  std::unordered_map<std::string, std::string> multi_max_sec_values_;
  std::unordered_map<std::string, uint64_t> multi_max_sec_seq_numbers_;
  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  bool pending_index_entry;
  BlockHandle pending_handle; // Handle to add to index block

  std::string compressed_output;

  // Add Min Max and MaxTimestamp secondary attribute value of a Block for
  // Interval Tree
  std::string maxSecValue;
  std::string minSecValue;
  uint64_t maxSecSeqNumber;

  Rep(const Options &opt, WritableFile *f)
      : options(opt), index_block_options(opt), interval_block_options(opt),
        file(f), offset(0), data_block(&options),
        index_block(&index_block_options),
        interval_block(&interval_block_options), num_entries(0), closed(false),
        filter_block(opt.filter_policy == NULL
                         ? NULL
                         : new FilterBlockBuilder(opt.filter_policy)),
        secondary_filter_block(opt.filter_policy == NULL
                                   ? NULL
                                   : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false), maxSecValue(""), minSecValue(""),
        maxSecSeqNumber(0) {
    index_block_options.block_restart_interval = 1;
    interval_block_options.block_restart_interval = 1;
    
    // NEW: Initialize multiple secondary indexes
    if (opt.filter_policy != NULL) {
      for (const auto& sec_key : opt.secondary_keys) {
        multi_secondary_filter_blocks_[sec_key] = new FilterBlockBuilder(opt.filter_policy);
        multi_interval_blocks_[sec_key] = new BlockBuilder(&interval_block_options);
        multi_min_sec_values_[sec_key] = "";
        multi_max_sec_values_[sec_key] = "";
        multi_max_sec_seq_numbers_[sec_key] = 0;
      }
    }
  }
};

TableBuilder::TableBuilder(const Options &options, WritableFile *file,
                           TwoDITwTopK *intervalTree, uint64_t fileNumber,
                           std::string *minsec, std::string *maxsec)

    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != NULL) {
    rep_->filter_block->StartBlock(0);
  }
  if (rep_->secondary_filter_block != NULL) {
    rep_->secondary_filter_block->StartBlock(0);
  }

  // NEW: Initialize StartBlock(0) for all multi-secondary filter blocks
  for (auto& [key, filter_block] : rep_->multi_secondary_filter_blocks_) {
    if (filter_block != NULL) {
      filter_block->StartBlock(0);
    }
  }

  intervalTree_ = intervalTree;
  this->fileNumber = fileNumber;
  this->smallest_sec = minsec;
  this->largest_sec = maxsec;
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed); // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_->secondary_filter_block;
  
  // NEW: Clean up multiple secondary indexes
  for (auto& [key, filter_block] : rep_->multi_secondary_filter_blocks_) {
    delete filter_block;
  }
  for (auto& [key, interval_block] : rep_->multi_interval_blocks_) {
    delete interval_block;
  }
  
  delete rep_;
}

Status TableBuilder::ChangeOptions(const Options &options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  rep_->interval_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice &key, const Slice &value) {

  Rep *r = rep_;
  assert(!r->closed);
  if (!ok())
    return;
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  if (r->pending_index_entry) {
    // cout<<"last key: "<<r->last_key<<endl;

    if (!r->options.interval_tree_file_name.empty())
      intervalTree_->insertInterval(
          ToString(fileNumber) + "+" +
              r->last_key.substr(0, r->last_key.size() - 8),
          r->minSecValue, r->maxSecValue, r->maxSecSeqNumber);
    else {
      // string s = rep_->minSecValue + "," + rep_->maxSecValue+ "," +
      // std::to_string(rep_->maxSecSeqNumber);
      Slice value(r->maxSecValue); //"1,1,1";
      Slice inKey(r->minSecValue);

      r->interval_block.Add(inKey, value);

      // r->interval_block.Add(std::to_string(count++), value);
    }
    
    // NEW: Process multiple secondary indexes interval data
    for (const auto& sec_key : r->options.secondary_keys) {
      const auto& min_val = r->multi_min_sec_values_[sec_key];
      const auto& max_val = r->multi_max_sec_values_[sec_key];
      const auto& max_seq = r->multi_max_sec_seq_numbers_[sec_key];
      
      if (!min_val.empty() && !max_val.empty()) {
        if (!r->options.interval_tree_file_name.empty()) {
          intervalTree_->insertInterval(
              ToString(fileNumber) + "+" + sec_key + "+" +
                  r->last_key.substr(0, r->last_key.size() - 8),
              min_val, max_val, max_seq);
        } else {
          auto interval_it = r->multi_interval_blocks_.find(sec_key);
          if (interval_it != r->multi_interval_blocks_.end()) {
            Slice value(max_val);
            Slice inKey(min_val);
            interval_it->second->Add(inKey, value);
          }
        }
      }
    }

    if (smallest_sec->empty()) {
      smallest_sec->assign(r->minSecValue);
      largest_sec->assign(r->maxSecValue);
    } else {
      if (this->smallest_sec->compare(r->minSecValue) > 0) {
        this->smallest_sec->assign(r->minSecValue);
      } else if (this->largest_sec->compare(rep_->maxSecValue) < 0) {
        this->largest_sec->assign(r->maxSecValue);
      }
    }

    r->minSecValue.clear();
    r->maxSecValue.clear();
    r->maxSecSeqNumber = 0;

    // NEW: Clear multiple secondary indexes interval data
    for (const auto& sec_key : r->options.secondary_keys) {
      r->multi_min_sec_values_[sec_key].clear();
      r->multi_max_sec_values_[sec_key].clear();
      r->multi_max_sec_seq_numbers_[sec_key] = 0;
    }

    assert(r->data_block.empty());
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    r->pending_index_entry = false;
  }

  if (r->filter_block != NULL) {
    r->filter_block->AddKey(key);
  }

  if (r->secondary_filter_block != NULL && !r->options.secondary_key.empty()) {
    rapidjson::Document docToParse;
    docToParse.Parse<0>(value.ToString().c_str());
    const char *sKeyAtt = r->options.secondary_key.c_str();
    if (!docToParse.IsObject() || !docToParse.HasMember(sKeyAtt) ||
        docToParse[sKeyAtt].IsNull())
      return;

    std::ostringstream sKey;
    if (docToParse[sKeyAtt].IsNumber()) {
      if (docToParse[sKeyAtt].IsUint64()) {
        unsigned long long int tid = docToParse[sKeyAtt].GetUint64();
        sKey << tid;
      } else if (docToParse[sKeyAtt].IsInt64()) {
        long long int tid = docToParse[sKeyAtt].GetInt64();
        sKey << tid;
      } else if (docToParse[sKeyAtt].IsDouble()) {
        double tid = docToParse[sKeyAtt].GetDouble();
        sKey << tid;
      }
      else if (docToParse[sKeyAtt].IsUint()) {
        unsigned int tid = docToParse[sKeyAtt].GetUint();
        sKey << tid;
      } else if (docToParse[sKeyAtt].IsInt()) {
        int tid = docToParse[sKeyAtt].GetInt();
        sKey << tid;
      }
    } else if (docToParse[sKeyAtt].IsString()) {
      const char *tid = docToParse[sKeyAtt].GetString();
      sKey << tid;
    } else if (docToParse[sKeyAtt].IsBool()) {
      bool tid = docToParse[sKeyAtt].GetBool();
      sKey << tid;
    }
    std::string tag = key.ToString().substr(key.size() - 8);
    std::string secKeys = sKey.str();

    Slice Key = secKeys + tag;

    r->secondary_filter_block->AddKey(Key);
    if (r->maxSecValue == "" || r->maxSecValue.compare(secKeys) < 0) {
      r->maxSecValue.assign(secKeys);
    }

    if (r->minSecValue == "" || r->minSecValue.compare(secKeys) > 0) {
      r->minSecValue.assign(secKeys);
    }
  }

  // NEW: Process multiple secondary indexes
  if (!r->options.secondary_keys.empty()) {
    rapidjson::Document docToParse;
    docToParse.Parse<0>(value.ToString().c_str());
    if (docToParse.IsObject()) {
      for (const auto& sec_key : r->options.secondary_keys) {
        if (!docToParse.HasMember(sec_key.c_str()) || docToParse[sec_key.c_str()].IsNull()) {
          continue;
        }
        // Extract secondary key value
        std::ostringstream sKey;
        const auto& sec_value = docToParse[sec_key.c_str()];
        
        if (sec_value.IsNumber()) {
          if (sec_value.IsUint64()) {
            sKey << sec_value.GetUint64();
          } else if (sec_value.IsInt64()) {
            sKey << sec_value.GetInt64();
          } else if (sec_value.IsDouble()) {
            sKey << sec_value.GetDouble();
          } else if (sec_value.IsUint()) {
            sKey << sec_value.GetUint();
          } else if (sec_value.IsInt()) {
            sKey << sec_value.GetInt();
          }
        } else if (sec_value.IsString()) {
          sKey << sec_value.GetString();
        } else if (sec_value.IsBool()) {
          sKey << (sec_value.GetBool() ? "true" : "false");
        }

        std::string tag = key.ToString().substr(key.size() - 8);
        std::string secKeys = sKey.str();
        Slice Key = secKeys + tag;

        // Add to filter block
        auto filter_it = r->multi_secondary_filter_blocks_.find(sec_key);
        if (filter_it != r->multi_secondary_filter_blocks_.end()) {
          filter_it->second->AddKey(Key);
        }

        // Update min/max values for interval blocks
        auto& min_val = r->multi_min_sec_values_[sec_key];
        auto& max_val = r->multi_max_sec_values_[sec_key];
        
        if (min_val.empty() || min_val.compare(secKeys) > 0) {
          min_val.assign(secKeys);
        }
        if (max_val.empty() || max_val.compare(secKeys) < 0) {
          max_val.assign(secKeys);
        }

        // Update sequence number
        const size_t n = key.size();
        if (n >= 8) {
          uint64_t num = DecodeFixed64(key.data() + n - 8);
          SequenceNumber seq = num >> 8;
          if (r->multi_max_sec_seq_numbers_[sec_key] < seq) {
            r->multi_max_sec_seq_numbers_[sec_key] = seq;
          }
        }
      }
    }
  }

  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);

  const size_t n = key.size();
  if (n >= 8) {
    uint64_t num = DecodeFixed64(key.data() + n - 8);
    SequenceNumber seq = num >> 8;
    if (r->maxSecSeqNumber < seq) {
      r->maxSecSeqNumber = seq;
    }
  }

  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep *r = rep_;
  assert(!r->closed);
  if (!ok())
    return;
  if (r->data_block.empty())
    return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    r->pending_index_entry = true;
    r->status = r->file->Flush();
  }
  if (r->filter_block != NULL) {
    r->filter_block->StartBlock(r->offset);
  }
  if (r->secondary_filter_block != NULL) {
    r->secondary_filter_block->StartBlock(r->offset);
  }
  
  // NEW: Start blocks for multiple secondary filters
  for (auto& [key, filter_block] : r->multi_secondary_filter_blocks_) {
    filter_block->StartBlock(r->offset);
  }
}

void TableBuilder::WriteBlock(BlockBuilder *block, BlockHandle *handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep *r = rep_;
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
  case kNoCompression:
    block_contents = raw;
    break;

  case kSnappyCompression: {
    std::string *compressed = &r->compressed_output;
    if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
        compressed->size() < raw.size() - (raw.size() / 8u)) {
      block_contents = *compressed;
    } else {
      // Snappy not supported, or compressed less than 12.5%, so just
      // store uncompressed form
      block_contents = raw;
      type = kNoCompression;
    }
    break;
  }
  }
  WriteRawBlock(block_contents, type, handle);
  r->compressed_output.clear();
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice &block_contents,
                                 CompressionType type, BlockHandle *handle) {
  Rep *r = rep_;
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1); // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish() {
  Rep *r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, secondary_filter_block_handle,
      metaindex_block_handle, index_block_handle, interval_block_handle;

  // Write filter block
  if (ok() && r->filter_block != NULL) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }
  // Write Secondary Filer Block
  if (ok() && r->secondary_filter_block != NULL) {
    WriteRawBlock(r->secondary_filter_block->Finish(), kNoCompression,
                  &secondary_filter_block_handle);
  }
  
  // NEW: Write multiple secondary filter blocks
  std::unordered_map<std::string, BlockHandle> multi_filter_handles;
  std::unordered_map<std::string, BlockHandle> multi_interval_handles;
  
  for (auto& [sec_key, filter_block] : r->multi_secondary_filter_blocks_) {
    // Only write non-empty filter blocks
    if (filter_block != NULL) {
      BlockHandle handle;
      WriteRawBlock(filter_block->Finish(), kNoCompression, &handle);
      multi_filter_handles[sec_key] = handle;
    }
  }
  
  // Write metaindex block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != NULL) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }
    if (r->secondary_filter_block != NULL) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "secondaryfilter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      secondary_filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    for (auto& [sec_key, filter_block] : multi_filter_handles) {
      std::string key = "secondaryfilter." + sec_key;
      std::string handle_encoding;
      filter_block.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  if (ok()) {
    if (r->pending_index_entry) {
      if (!r->options.interval_tree_file_name.empty()) {
        intervalTree_->insertInterval(
            ToString(fileNumber) + "+" +
                r->last_key.substr(0, r->last_key.size() - 8),
            r->minSecValue, r->maxSecValue, r->maxSecSeqNumber);
        intervalTree_->sync();
      } else {
        // string s = rep_->minSecValue + "," + rep_->maxSecValue+ "," +
        // std::to_string(rep_->maxSecSeqNumber); cout<<s;
        Slice value(r->maxSecValue); //"1,1,1";
        Slice inKey(r->minSecValue);

        r->interval_block.Add(inKey, value);
      }

      if (smallest_sec->empty()) {
        smallest_sec->assign(r->minSecValue);
        largest_sec->assign(r->maxSecValue);
      } else {
        if (this->smallest_sec->compare(r->minSecValue) > 0) {
          this->smallest_sec->assign(r->minSecValue);
        } else if (this->largest_sec->compare(rep_->maxSecValue) < 0) {
          this->largest_sec->assign(r->maxSecValue);
        }
      }

      // outputFile<<SSTR(fileNumber)+"+"+ r->last_key.substr(0,
      // r->last_key.size() - 8 )<<" "<< rep_->minSecValue <<" "<<
      // rep_->maxSecValue<<" "<< rep_->maxSecSeqNumber<<std::endl;
      r->minSecValue.clear();
      r->maxSecValue.clear();
      r->maxSecSeqNumber = 0;

      // NEW: Clear multiple secondary indexes interval data
      for (const auto& sec_key : r->options.secondary_keys) {
        r->multi_min_sec_values_[sec_key].clear();
        r->multi_max_sec_values_[sec_key].clear();
        r->multi_max_sec_seq_numbers_[sec_key] = 0;
      }

      // outputFile<<r->last_key<<std::endl;
      // r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;

      //      if(rand()%100==0)
      //    	  intervalTree_->storagePrint();
    }

    // Write index block

    if (r->options.interval_tree_file_name.empty()) {
      WriteBlock(&r->interval_block, &interval_block_handle);
    }
    
    // NEW: Write multiple interval blocks
    for (auto& [sec_key, interval_block] : r->multi_interval_blocks_) {
      // Only write non-empty interval blocks
      if (!interval_block->empty()) {
        BlockHandle handle;
        WriteBlock(interval_block, &handle);
        multi_interval_handles[sec_key] = handle;
      }
    }
    
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // Write footer
  if (ok()) {
    Footer footer;
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    bool isinterval = r->options.interval_tree_file_name.empty();
    if (isinterval) {
      footer.set_interval_handle(interval_block_handle);
    }
    
    // NEW: Set multiple interval handles
    footer.set_multi_interval_handles(multi_interval_handles);
    
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding, isinterval);
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      r->offset += footer_encoding.size();
    }
  }

  return r->status;
}

void TableBuilder::Abandon() {
  Rep *r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

} // namespace leveldb
