// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_DB_IMPL_H_
#define STORAGE_LEVELDB_DB_DB_IMPL_H_

#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/snapshot.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "port/port.h"
#include "port/thread_annotations.h"
#include <deque>
#include <fstream>
#include <set>

namespace leveldb {

class MemTable;
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

// IOStat DB::iostat;

class IOStat {
public:
  uint64_t numberofIO;
  uint64_t cachehit;
  uint64_t prunebloomfilter;
  uint64_t bloomfilterFP;
  uint64_t pruneinterval;
  uint64_t prunefile;
  IOStat() { clear(); }
  void clear() {
    numberofIO = 0;
    cachehit = 0;
    prunebloomfilter = 0;
    bloomfilterFP = 0;
    pruneinterval = 0;
    prunefile = 0;
  }

  void update(IOStat &newstat) {
    numberofIO += newstat.numberofIO;
    cachehit += newstat.cachehit;
    prunebloomfilter += newstat.prunebloomfilter;
    bloomfilterFP += newstat.bloomfilterFP;
    pruneinterval += newstat.pruneinterval;
    prunefile += newstat.prunefile;
  }
  void print() {
    // std::cout<<"asdasd";
    std::cout << "IO: " << this->numberofIO << std::endl;
    std::cout << "Cache Hit: " << this->cachehit << std::endl;
    std::cout << "BF Prune: " << this->prunebloomfilter << std::endl;
    std::cout << "BF FP: " << this->bloomfilterFP << std::endl;
    std::cout << "RF Prune: " << this->pruneinterval << std::endl;
    std::cout << "RF File PRune: " << this->prunefile << std::endl;
  }
  void print(uint64_t numberofop) {
    std::cout << "Total IO: " << this->numberofIO << std::endl;
    std::cout << "Avg IO: " << (double)this->numberofIO / numberofop
              << std::endl;
    if (numberofIO > 0) {
      double s = (double)cachehit / numberofIO * 100.0;
      std::cout << "Cache Hit%: " << s << std::endl;
    } else
      std::cout << "Cache Hit%: 0" << std::endl;
    std::cout << "BF Prune: " << this->prunebloomfilter << std::endl;
    cout.precision(6);
    if (this->bloomfilterFP + this->prunebloomfilter > 0)
      std::cout << "BF FP%: "
                << (double)this->bloomfilterFP /
                       (double)(this->bloomfilterFP + this->prunebloomfilter) *
                       100.0
                << std::endl;
    else
      std::cout << "BF FP%: 0" << std::endl;
    // cout.precision(3);
    std::cout << "Avg RF Prune: " << this->pruneinterval / numberofop
              << std::endl;
    std::cout << "Avg RF File Prune: " << this->prunefile / numberofop
              << std::endl;
  }
};

class DBImpl : public DB {
public:
  DBImpl(const Options &options, const std::string &dbname);
  virtual ~DBImpl();

  // Implementations of the DB interface
  virtual Status Put(const WriteOptions &, const Slice &key,
                     const Slice &value);
  virtual Status Put(const WriteOptions &o, const Slice &val);
  virtual Status Delete(const WriteOptions &, const Slice &key);
  virtual Status Write(const WriteOptions &options, WriteBatch *updates);
  virtual Status Get(const ReadOptions &options, const Slice &key,
                     std::string *value);
  virtual Status Get(const ReadOptions &options, const Slice &skey,
                     std::vector<SecondayKeyReturnVal> *value,
                     int kNoOfOutputs);

  virtual bool checkifValid(const ReadOptions &options, const Slice &key,
                            int &level);

  virtual Status RangeGet(const ReadOptions &options, const Slice &startSkey,
                          const Slice &endSkey,
                          std::vector<SecondayKeyReturnVal> *value,
                          int kNoOfOutputs);
  virtual Iterator *NewIterator(const ReadOptions &);
  virtual const Snapshot *GetSnapshot();
  virtual void ReleaseSnapshot(const Snapshot *snapshot);
  virtual bool GetProperty(const Slice &property, std::string *value);
  virtual void GetApproximateSizes(const Range *range, int n, uint64_t *sizes);
  virtual void CompactRange(const Slice *begin, const Slice *end);

  // Extra methods (for testing) that are not in the public DB interface

  // Compact any files in the named level that overlap [*begin,*end]
  void TEST_CompactRange(int level, const Slice *begin, const Slice *end);

  // Force current memtable contents to be compacted.
  Status TEST_CompactMemTable();

  // Return an internal iterator over the current state of the database.
  // The keys of this iterator are internal keys (see format.h).
  // The returned iterator should be deleted when no longer needed.
  Iterator *TEST_NewInternalIterator();

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  int64_t TEST_MaxNextLevelOverlappingBytes();

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.
  void RecordReadSample(Slice key);

private:
  friend class DB;
  struct CompactionState;
  struct Writer;

  Iterator *NewInternalIterator(const ReadOptions &,
                                SequenceNumber *latest_snapshot,
                                uint32_t *seed);

  Status NewDB();

  // Recover the descriptor from persistent storage.  May do a significant
  // amount of work to recover recently logged updates.  Any changes to
  // be made to the descriptor are added to *edit.
  Status Recover(VersionEdit *edit) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  void MaybeIgnoreError(Status *s) const;

  // Delete any unneeded files and stale in-memory entries.
  void DeleteObsoleteFiles();

  // Compact the in-memory write buffer to disk.  Switches to a new
  // log-file/memtable and writes a new descriptor iff successful.
  Status CompactMemTable() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status RecoverLogFile(uint64_t log_number, VersionEdit *edit,
                        SequenceNumber *max_sequence)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status WriteLevel0Table(MemTable *mem, VersionEdit *edit, Version *base)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status MakeRoomForWrite(bool force /* compact even if there is room? */)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  WriteBatch *BuildBatchGroup(Writer **last_writer);

  void MaybeScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  static void BGWork(void *db);
  void BackgroundCall();
  Status BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  void CleanupCompaction(CompactionState *compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
  Status DoCompactionWork(CompactionState *compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Status OpenCompactionOutputFile(CompactionState *compact);
  Status FinishCompactionOutputFile(CompactionState *compact, Iterator *input);
  Status InstallCompactionResults(CompactionState *compact)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Constant after construction
  Env *const env_;
  const InternalKeyComparator internal_comparator_;
  const InternalFilterPolicy internal_filter_policy_;
  const Options options_; // options_.comparator == &internal_comparator_
  bool owns_info_log_;
  bool owns_cache_;
  const std::string dbname_;

  // table_cache_ provides its own synchronization
  TableCache *table_cache_;

  // Lock over the persistent DB state.  Non-NULL iff successfully acquired.
  FileLock *db_lock_;

  // State below is protected by mutex_
  port::Mutex mutex_;
  port::AtomicPointer shutting_down_;
  port::CondVar bg_cv_; // Signalled when background work finishes
  MemTable *mem_;
  MemTable *imm_;               // Memtable being compacted
  port::AtomicPointer has_imm_; // So bg thread can detect non-NULL imm_
  WritableFile *logfile_;
  uint64_t logfile_number_;
  log::Writer *log_;
  uint32_t seed_; // For sampling.

  // Queue of writers.
  std::deque<Writer *> writers_;
  WriteBatch *tmp_batch_;

  SnapshotList snapshots_;

  // Set of table files to protect from deletion because they are
  // part of ongoing compactions.
  std::set<uint64_t> pending_outputs_;

  // Has a background compaction been scheduled or is running?
  bool bg_compaction_scheduled_;

  // Information for a manual compaction
  struct ManualCompaction {
    int level;
    bool done;
    const InternalKey *begin; // NULL means beginning of key range
    const InternalKey *end;   // NULL means end of key range
    InternalKey tmp_storage;  // Used to keep track of compaction progress
  };
  ManualCompaction *manual_compaction_;

  VersionSet *versions_;

  // Have we encountered a background error in paranoid mode?
  Status bg_error_;
  int consecutive_compaction_errors_;

  // Per level compaction stats.  stats_[level] stores the stats for
  // compactions that produced data for the specified "level".
  struct CompactionStats {
    int64_t micros;
    int64_t bytes_read;
    int64_t bytes_written;

    CompactionStats() : micros(0), bytes_read(0), bytes_written(0) {}

    void Add(const CompactionStats &c) {
      this->micros += c.micros;
      this->bytes_read += c.bytes_read;
      this->bytes_written += c.bytes_written;
    }
  };
  CompactionStats stats_[config::kNumLevels];

  // No copying allowed
  DBImpl(const DBImpl &);
  void operator=(const DBImpl &);

  const Comparator *user_comparator() const {
    return internal_comparator_.user_comparator();
  }
};

// Sanitize db options.  The caller should delete result.info_log if
// it is not equal to src.info_log.
extern Options SanitizeOptions(const std::string &db,
                               const InternalKeyComparator *icmp,
                               const InternalFilterPolicy *ipolicy,
                               const Options &src);

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_DB_IMPL_H_
