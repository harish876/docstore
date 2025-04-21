#include "leveldb/cache.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <leveldb/db.h>
#include <leveldb/filter_policy.h>
#include <leveldb/slice.h>
#include <leveldb/table.h>
#include <sstream>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>

using namespace std;
using namespace leveldb;

#define NUM_BIN 10

// NYTweetsUserIDRangerw19
// NYTweetsTimerw19

// static int workloadtype = 2; //static =1, dynamic =2
static string basefilepath =
    "/home/mohiuddin/Desktop/LevelDB_Correctness_Testing/";
// static string basefilepath = "../";

static string benchmark_file =
    basefilepath + "Benchmarks/NYTweetsUserIDRangerw19";

// static string dynamic_workload = basefilepath + "Datasets/WriteHeavy";
// static string dynamic_workload = basefilepath + "Datasets/ReadHeavy";
static string dynamic_workload = basefilepath + "Datasets/Large/UpdateHeavy";
static string dynamic_database =
    basefilepath + "DB/Embedded_Dynamic_Update_UserID";
static string dynamic_result_file =
    basefilepath + "Results/Embedded_Dynamic_Update.csv";
static string dynamic_iostat_file =
    basefilepath + "Results/Embedded_Dynamic_Update_iostat.csv";
static string dynamic_hist_file =
    basefilepath + "Results/Embedded_Dynamic_Update_UserID_hist";

static uint64_t MAX_OP = 1000000;
static uint64_t DYNAMIC_LOG_POINT = MAX_OP / 10;

static string database = basefilepath + "DB/Embedded_Static_Time_FATKV";
static string static_dataset = basefilepath + "Datasets/StaticDatasetLarge";
static string static_query = basefilepath + "Datasets/StaticQueryTime";
static string static_pr = basefilepath + "Datasets/StaticQueryID";

static string result_file =
    basefilepath + "Results/Embedded_Static_Time_FATKV_Lookup.csv";
static string iostat_file =
    basefilepath + "Results/Embedded_Static_Time_iostat_FATKV_Lookup.csv";
static string result_hist_file =
    basefilepath + "Results/Embedded_Static_Time_hist_";

static uint64_t MAX_WRITE = 200000000;
static uint64_t LOG_POINT = MAX_WRITE / 200;

static uint64_t MAX_QUERY = 10000;
static int filltablecache = 500;
static uint64_t READ_LOG_POINT = (MAX_QUERY - filltablecache) / 100;

static uint64_t MAX_QUERY_LOOKUP = 2000; // 10500;
static int filltablecacheL = 500;        // 500;
static uint64_t READ_LOG_POINT_L = (MAX_QUERY_LOOKUP) / 20;

static uint64_t MAX_QUERY_RL = 150;
static int filltablecacheRL = 0;
static uint64_t READ_LOG_POINT_RL = (MAX_QUERY_RL) / 15;

static uint64_t topkset[] = {10, 100, 1000000};
static int topksetlen = 3;

// static uint64_t SAMPLE_FOR_HIST =  200;//MAX_QUERY/10;

static std::string PrimaryAtt = "ID";
// static std::string SecondaryAtt = "CreationTime";
static std::string SecondaryAtt = "UserID";

static int isintervaltree = false;
static int numberofselectivity = 4;
static int topk = 10;

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

void ParseTweetJson(std::string &tweet, std::string &pkey) {

  if (PrimaryAtt.empty())
    pkey = "";
  rapidjson::Document docToParse;

  docToParse.Parse<0>(tweet.c_str());

  const char *pKeyAtt = PrimaryAtt.c_str();

  if (!docToParse.IsObject() || !docToParse.HasMember(pKeyAtt) ||
      docToParse[pKeyAtt].IsNull())
    pkey = "";

  std::ostringstream pKey;
  if (docToParse[pKeyAtt].IsNumber()) {
    if (docToParse[pKeyAtt].IsUint64()) {
      unsigned long long int tid = docToParse[pKeyAtt].GetUint64();
      pKey << tid;

    } else if (docToParse[pKeyAtt].IsInt64()) {
      long long int tid = docToParse[pKeyAtt].GetInt64();
      pKey << tid;
    } else if (docToParse[pKeyAtt].IsDouble()) {
      double tid = docToParse[pKeyAtt].GetDouble();
      pKey << tid;
    }

    else if (docToParse[pKeyAtt].IsUint()) {
      unsigned int tid = docToParse[pKeyAtt].GetUint();
      pKey << tid;
    } else if (docToParse[pKeyAtt].IsInt()) {
      int tid = docToParse[pKeyAtt].GetInt();
      pKey << tid;
    }
  } else if (docToParse[pKeyAtt].IsString()) {
    const char *tid = docToParse[pKeyAtt].GetString();
    pKey << tid;
  } else if (docToParse[pKeyAtt].IsBool()) {
    bool tid = docToParse[pKeyAtt].GetBool();
    pKey << tid;
  }

  // Slice key = pKey.str();
  docToParse.RemoveMember(PrimaryAtt.c_str());
  rapidjson::StringBuffer strbuf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(strbuf);
  docToParse.Accept(writer);

  tweet = strbuf.GetString();
  // cout<<pKey.str()<<endl<<endl;
  pkey = pKey.str();
}

void PerformDynamicWorkload() {
  leveldb::DB *db;
  leveldb::Options options;

  leveldb::IOStat lookup_iostat;
  leveldb::IOStat range_lookup_iostat;
  leveldb::IOStat get_iostat;

  w_iostat.clear();
  pr_iostat.clear();
  sr_iostat.clear();
  sr_range_iostat.clear();

  ofstream ofile_iostat(dynamic_iostat_file.c_str(),
                        std::ofstream::out | std::ofstream::app);

  int bloomfilter = 100;
  string sbloomfilter = "100";

  options.filter_policy = leveldb::NewBloomFilterPolicy(bloomfilter);
  options.PrimaryAtt = PrimaryAtt;
  options.secondaryAtt = SecondaryAtt;
  options.create_if_missing = true;
  options.max_open_files = 30000;
  options.block_cache = leveldb::NewLRUCache(200 * 1048576); // 200MB cache

  if (isintervaltree)
    options.interval_tree_file_name = "./interval_tree";
  else
    options.interval_tree_file_name = "";

  leveldb::Status status = leveldb::DB::Open(options, dynamic_database, &db);
  assert(status.ok());

  ifstream ifile(dynamic_workload.c_str());
  vector<leveldb::SKeyReturnVal> svalues;
  if (!ifile) {
    cerr << "Can't open input file " << endl;
    return;
  }
  string line;
  uint64_t i = 0;
  rapidjson::Document d;

  leveldb::ReadOptions roptions;

  double w = 0, rs = 0, rp = 0, rsrange = 0;
  long rscount = 0;
  double durationW = 0, durationRS = 0, durationRP = 0;
  ofstream ofile(dynamic_result_file.c_str(),
                 std::ofstream::out | std::ofstream::app);
  ofstream ofile_hist(dynamic_hist_file.c_str(),
                      std::ofstream::out | std::ofstream::app);

  while (getline(ifile, line)) {
    i++;

    std::vector<std::string> x = split(line, '\t');
    leveldb::WriteOptions woptions;
    struct timeval start, end;
    if (x.size() == 2) {

      if (x[0] == "w" || x[0] == "up") {

        std::string pkey;
        ParseTweetJson(x[1], pkey);

        gettimeofday(&start, NULL);
        leveldb::Status s = db->Put(woptions, pkey, x[1]);
        //            	leveldb::Status s = db->Put(woptions,  x[2]);
        gettimeofday(&end, NULL);
        durationW += ((end.tv_sec * 1000000 + end.tv_usec) -
                      (start.tv_sec * 1000000 + start.tv_usec));
        w++;
      } else if (x[0] == "rs") {

        roptions.type = ReadType::SRead;

        gettimeofday(&start, NULL);
        leveldb::Status s = db->Get(roptions, x[1], &svalues, topk);
        gettimeofday(&end, NULL);

        double duration = ((end.tv_sec * 1000000 + end.tv_usec) -
                           (start.tv_sec * 1000000 + start.tv_usec));
        durationRS += duration;
        int size = svalues.size();
        rscount += size;
        if (svalues.size() > 0) {
          svalues.clear();
        }
        rs++;

        ofile_hist << duration << endl;

      } else if (x[0] == "rp") {
        std::string pvalue;
        roptions.type = ReadType::PRead;
        leveldb::Status s = db->Get(roptions, x[1], &pvalue);
        gettimeofday(&end, NULL);
        durationRP += ((end.tv_sec * 1000000 + end.tv_usec) -
                       (start.tv_sec * 1000000 + start.tv_usec));
        // get_iostat.update(leveldb::iostat);
        rp++;
      }
    }
    if (i % DYNAMIC_LOG_POINT == 0) {

      if (i / DYNAMIC_LOG_POINT == 1) {
        ofile << "No of Op (Millions)" << "," << "Time Per Op." << ","
              << "Time Per Write" << "," << "Time Per Read" << ","
              << "Time Per Lookup" << "," << "Results Per Lookup" << endl;

        cout << "No of Op (Millions)" << "," << "Time Per Op." << ","
             << "Time Per Write" << "," << "Time Per Read" << ","
             << "Time Per Lookup" << "," << "Results Per Lookup" << endl;

        sr_iostat.print(0, ofile_iostat, "");
      }
      cout << fixed;
      cout.precision(3);

      ofile << fixed;
      ofile.precision(3);
      ofile << i / DYNAMIC_LOG_POINT << ","
            << (durationW + durationRP + durationRS) / i << "," << durationW / w
            << "," << durationRP / rp << "," << durationRS / rs << ","
            << rscount / rs << endl;
      cout << i / DYNAMIC_LOG_POINT << ",\t"
           << (durationW + durationRP + durationRS) / i << ",\t"
           << durationW / w << ",\t" << durationRP / rp << ",\t"
           << durationRS / rs << ",\t" << rscount / rs << endl;

      // cout<< "\nLookup IOStat\n\n";
      // printiostat(lookup_iostat, rs);
      sr_iostat.print(rs, ofile_iostat, "Lookup");
      sr_iostat.print(rs);

      // cout<< "\nRange Lookup IOStat\n\n";
      // sr_range_iostat.print(rsrange, ofile_iostat, "RangeLookup");
      // printiostat(range_lookup_iostat, rsrange);

      // cout<< "\nGET IOStat\n\n";
      pr_iostat.print(rp, ofile_iostat, "Get");
      // printiostat(get_iostat, rp);

      // cout<< "\nWrite IOStat\n\n";
      w_iostat.print(w, ofile_iostat, "Put");

      if (i >= MAX_OP)
        break;
    }
  }

  delete db;
  delete options.filter_policy;
  delete options.block_cache;
  ofile.close();
  ofile_iostat.close();
  ofile_hist.close();
}

// selectivityfactor<0  means no lookups, 0 is lookup and > 0 is rangelookup
void PerformStaticWorkload(bool put, bool get, int selectivityfactor) {
  leveldb::DB *db;
  leveldb::Options options;

  w_iostat.clear();
  pr_iostat.clear();
  sr_iostat.clear();
  sr_range_iostat.clear();

  ofstream ofile_iostat(iostat_file.c_str(),
                        std::ofstream::out | std::ofstream::app);

  int bloomfilter = 100;

  options.filter_policy = leveldb::NewBloomFilterPolicy(bloomfilter);
  options.max_open_files = 30000;
  options.block_cache = leveldb::NewLRUCache(200 * 1048576); // 200MB cache

  options.PrimaryAtt = PrimaryAtt;
  options.secondaryAtt = SecondaryAtt;
  options.create_if_missing = true;
  if (isintervaltree)
    options.interval_tree_file_name = "./interval_tree";
  else
    options.interval_tree_file_name = "";

  leveldb::Status status = leveldb::DB::Open(options, database, &db);
  assert(status.ok());

  string line;
  uint64_t i = 0;
  rapidjson::Document d;

  leveldb::ReadOptions roptions;
  // roptions.iostat.clear();

  uint64_t w = 0, rs = 0, rp = 0;
  double rscount = 0;
  double durationW = 0, durationRS = 0, durationRP = 0;

  // double durationRSHist[NUM_BIN];

  struct timeval start, end;

  ofstream ofile(result_file.c_str(), std::ofstream::out | std::ofstream::app);

  // write all records
  if (put) {

    ifstream ifile(static_dataset.c_str());
    if (!ifile) {
      cerr << "Can't open input file " << endl;
      return;
    }

    ofile << "Op Type" << "," << "No of Op (Millions)" << "," << "Time Per Op."
          << "," << "Results Per Op" << endl;

    w_iostat.print(0, ofile_iostat, "");

    std::string tweet;
    while (getline(ifile, tweet)) {
      i++;
      leveldb::WriteOptions woptions;

      std::string pkey;
      ParseTweetJson(tweet, pkey);

      gettimeofday(&start, NULL);
      leveldb::Status s = db->Put(woptions, pkey, tweet);
      gettimeofday(&end, NULL);

      durationW += ((end.tv_sec * 1000000 + end.tv_usec) -
                    (start.tv_sec * 1000000 + start.tv_usec));
      w++;

      if (w % LOG_POINT == 0) {
        ofile << fixed;
        ofile.precision(3);
        ofile << "Put," << w / LOG_POINT << "," << durationW / w << ",0"
              << endl;

        cout << "Put," << w / LOG_POINT << "," << durationW / w << ",0" << endl;
        w_iostat.print(w, ofile_iostat, "Put");

        if (w >= MAX_WRITE)
          break;

        // usleep(1000);
      }
    }
    ifile.close();
  }

  // End of All writes, Read on Primary Begins
  if (get) {
    std::string pread;
    ifstream ifile_pr(static_pr.c_str());
    if (!ifile_pr) {
      cerr << "Can't open query file " << endl;
      return;
    }
    vector<leveldb::SKeyReturnVal> svalues;
    i = 0;
    while (getline(ifile_pr, pread)) {
      i++;
      std::string pvalue;
      roptions.type = ReadType::PRead;
      gettimeofday(&start, NULL);
      leveldb::Status s = db->Get(roptions, pread, &pvalue);
      gettimeofday(&end, NULL);
      durationRP += ((end.tv_sec * 1000000 + end.tv_usec) -
                     (start.tv_sec * 1000000 + start.tv_usec));
      // get_iostat.update(leveldb::iostat);
      int found = 0;
      if (s.IsNotFound() == false)
        found = 1;
      rp++;

      if (rp % READ_LOG_POINT == 0) {
        ofile << fixed;
        ofile.precision(3);
        ofile << "Get," << rp / READ_LOG_POINT << "," << durationRP / rp << ","
              << found << endl;

        cout << "Get," << rp / READ_LOG_POINT << "," << durationRP / rp << ","
             << found << endl;
        pr_iostat.print(rp, ofile_iostat, "Get");

        if (rp >= MAX_QUERY)
          break;
      }
    }

    ifile_pr.close();
  }

  // End of All Reads, Read on Secondary Lookup Begins
  int readop = 0;
  if (selectivityfactor >= 0) {
    std::string query;
    // int selectivity=0;

    ifstream ifile_q(static_query.c_str());
    if (!ifile_q) {
      cerr << "Can't open query file " << endl;
      return;
    }

    for (int s = 0; s < numberofselectivity; s++) {
      if (selectivityfactor != s && selectivityfactor < numberofselectivity)
        continue;

      for (int t = 0; t < topksetlen; t++) {
        // cout<<"\nInsert topk (insert 0 to end): "; cin>>topk;
        // cout<<"Insert selectivity, (0-1), 0 means Point Lookup, 1 means range
        // "; cin>>selectivity;

        topk = topkset[t];
        // roptions.num_records = topk;

        if (topk == 0)
          break;

        vector<leveldb::SKeyReturnVal> svalues;

        durationRS = 0, rs = 0, rscount = 0;
        // int durationRSHist[NUM_BIN]={0};
        sr_iostat.clear();
        sr_range_iostat.clear();

        ifile_q.seekg(0, ios::beg);

        //					double minDuration = 99999999999
        //, maxDuration=-1; 					int binsize = 0;

        string histfile;
        if (s == 0) {
          histfile = result_hist_file + "Lookup-k=" + to_string(topk);
          READ_LOG_POINT = READ_LOG_POINT_L;
          MAX_QUERY = MAX_QUERY_LOOKUP;
          filltablecache = filltablecacheL;

        } else {
          histfile = result_hist_file + "RangeLookup-k=" + to_string(topk) +
                     "sel=" + to_string(s);
          READ_LOG_POINT = READ_LOG_POINT_RL;
          MAX_QUERY = MAX_QUERY_RL;
          filltablecache = filltablecacheRL;
        }

        ofstream ofile_hist(histfile.c_str(), std::ofstream::out);

        while (getline(ifile_q, query)) {

          std::vector<std::string> x = split(query, '\t');

          if (x.size() == 3) {

            int sel = std::atoi(x[0].c_str());

            if (s != sel)
              continue;

            if (sel == 0) {

              roptions.type = ReadType::SRead;
              gettimeofday(&start, NULL);
              leveldb::Status s = db->Get(roptions, x[1], &svalues, topk);
              gettimeofday(&end, NULL);

              if (readop < filltablecache) {
                readop++;
                continue;
              }
            } else {
              roptions.type = ReadType::SRRead;

              gettimeofday(&start, NULL);
              leveldb::Status s =
                  db->RangeLookUp(roptions, x[1], x[2], &svalues, topk);
              gettimeofday(&end, NULL);

              if (readop < filltablecache) {
                readop++;
                continue;
              }
            }

            double duration = ((end.tv_sec * 1000000 + end.tv_usec) -
                               (start.tv_sec * 1000000 + start.tv_usec)) /
                              1000.0;
            durationRS += duration;
            int size = svalues.size();
            rscount += size;
            if (svalues.size() > 0) {
              svalues.clear();
            }
            rs++;
            ofile_hist << duration << endl;
            //							if(rs<SAMPLE_FOR_HIST)
            //							{
            //								if(duration
            //< minDuration )
            // minDuration =duration;
            // if(duration > maxDuration)
            // maxDuration = duration;
            //								//continue;
            //
            //							}
            //							else if(rs ==
            // SAMPLE_FOR_HIST)
            //							{
            //								binsize
            //= (int)(maxDuration - minDuration) / NUM_BIN;
            //
            //							}
            //							else
            {
              //								int
              // x= minDuration;
              // for(int b=0;b<NUM_BIN;b++)
              //								{
              //
              //									if(duration
              //<= x || b==NUM_BIN-1)
              //									{
              //										durationRSHist[b]++;
              //										break;
              //									}
              //
              //									x
              //+= binsize;
              //								}

              if (rs % READ_LOG_POINT == 0) {
                ofile << fixed;
                ofile.precision(3);
                string optype = "";
                if (s == 0) {
                  optype = "Lookup-k=" + to_string(topk);
                  sr_iostat.print(rs, ofile_iostat, optype.c_str());

                } else {
                  optype = "RangeLookup-k=" + to_string(topk) +
                           "sel=" + to_string(s);
                  sr_range_iostat.print(rs, ofile_iostat, optype.c_str());
                }
                ofile << optype << "," << rs / READ_LOG_POINT << ","
                      << durationRS / rs << "," << rscount / rs << endl;
                cout << rs << "," << optype << "," << rs / READ_LOG_POINT << ","
                     << durationRS / rs << "," << rscount / rs << endl;

                //									int
                // x = minDuration;
                // for (int b = 0; b < NUM_BIN; b++)
                //									{
                //										ofile
                //<< x <<","<<  durationRSHist[b] << ",";
                // cout << x <<","<< durationRSHist[b] << ",";
                //
                //										x
                //+= binsize;
                //
                //									}
                //									ofile<<endl;
                //									cout<<endl;

                if (rs >= MAX_QUERY)
                  break;
              }
            }
          }
        }

        ifile_q.clear();
        ofile_hist.close();
        // delete durationRSHist;
      }
    }

    ifile_q.close();
  }

  delete db;
  delete options.block_cache;
  delete options.filter_policy;
  ofile.close();
  ofile_iostat.close();
}

int main(int argc, char **argv) {

  if (argc == 4) {
    benchmark_file = argv[1];
    database = argv[2];
    result_file = argv[3];
    // cout<< argv[3];
    // testWithBenchMark();

  } else {
    // TestPerformStaticWorkload(true);

    PerformDynamicWorkload();
    // PerformStaticWorkload(false, false, 6);

    // cout<<"Please Put arguments in order: \n arg1=benchmarkpath arg2=dbpath
    // arg3=resultpath\n";
  }

  return 0;
}
