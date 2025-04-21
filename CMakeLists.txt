# Copyright (c) 2011 The LevelDB Authors. All rights reserved.
# Use of this source code is governed by a BSD-style license that can be
# found in the LICENSE file. See the AUTHORS file for names of contributors.

cmake_minimum_required(VERSION 3.10)

# Project definition with version from db.h
project(leveldb-EmbeddedIndex VERSION 1.14.0 LANGUAGES C CXX)

# Build options (similar to OPT in Makefile)
option(LEVELDB_PRODUCTION "Production use (optimized mode)" ON)
option(LEVELDB_DEBUG "Debug mode with full line-level debugging symbols" OFF)
option(LEVELDB_PROFILE "Profiling mode: optimized with debugging symbols" OFF)
option(LEVELDB_BUILD_TESTS "Build LevelDB's unit tests" OFF)
option(LEVELDB_BUILD_BENCHMARKS "Build LevelDB's benchmarks" OFF)
option(LEVELDB_HTTP_SERVER "Build HTTP server for LevelDB" ON)
include(GNUInstallDirs)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Set build type based on options
if(LEVELDB_DEBUG)
  set(CMAKE_BUILD_TYPE Debug)
  add_compile_options(-g2)
elseif(LEVELDB_PROFILE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
  add_compile_options(-O2 -g2 -DNDEBUG)
else() # Production is default
  set(CMAKE_BUILD_TYPE Release)
  add_compile_options(-O2 -DNDEBUG)
endif()

# Platform detection
if(WIN32)
  set(LEVELDB_PLATFORM "WINDOWS")
elseif(APPLE)
  set(LEVELDB_PLATFORM "MACOSX")
  # Check for iOS
  if(IOS)
    set(LEVELDB_PLATFORM "IOS")
  endif()
else()
  set(LEVELDB_PLATFORM "POSIX")
endif()

# Compiler settings
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wall -pedantic)

# Additional debug symbols (corresponding to -g in CXXFLAGS)
if(CMAKE_BUILD_TYPE MATCHES "Debug")
  add_compile_options(-g)
endif()

# Include directories
include_directories(
  .
  include
)

# Platform-specific settings
if(LEVELDB_PLATFORM STREQUAL "POSIX")
  add_compile_definitions(
    OS_LINUX
    LEVELDB_PLATFORM_POSIX
  )
  add_compile_options(-fno-builtin-memcmp)
  set(THREADS_PREFER_PTHREAD_FLAG ON)
  find_package(Threads REQUIRED)
endif()

# Source files (from build_config.mk)
set(LEVELDB_SOURCES
  # From TwoD_IT_w_TopK directory
  TwoD_IT_w_TopK/TwoD_IT_w_TopK.cc
  
  # DB sources
  db/builder.cc
  db/c.cc
  db/db_impl.cc
  db/db_iter.cc
  db/dbformat.cc
  db/filename.cc
  db/log_reader.cc
  db/log_writer.cc
  db/memtable.cc
  db/repair.cc
  db/table_cache.cc
  db/version_edit.cc
  db/version_set.cc
  db/write_batch.cc
  
  # Table sources
  table/block.cc
  table/block_builder.cc
  table/filter_block.cc
  table/format.cc
  table/iterator.cc
  table/merger.cc
  table/table.cc
  table/table_builder.cc
  table/two_level_iterator.cc
  
  # Util sources
  util/arena.cc
  util/bloom.cc
  util/cache.cc
  util/coding.cc
  util/comparator.cc
  util/crc32c.cc
  util/env.cc
  util/env_posix.cc
  util/filter_policy.cc
  util/hash.cc
  util/histogram.cc
  util/logging.cc
  util/options.cc
  util/status.cc
  
  # Port sources
  port/port_posix.cc
)

# MemEnv sources
set(MEMENV_SOURCES
  helpers/memenv/memenv.cc
)

# Test utility sources
set(TESTUTIL_SOURCES
  util/testutil.cc
)

set(TESTHARNESS_SOURCES
  util/testharness.cc
  ${TESTUTIL_SOURCES}
)

# Main library target
add_library(leveldb STATIC ${LEVELDB_SOURCES})
target_link_libraries(leveldb Threads::Threads)

# Shared library (if enabled)
option(BUILD_SHARED_LIBS "Build shared libraries" ON)
if(BUILD_SHARED_LIBS)
  add_library(leveldb-shared SHARED ${LEVELDB_SOURCES})
  target_link_libraries(leveldb-shared Threads::Threads)
  set_target_properties(leveldb-shared PROPERTIES 
    OUTPUT_NAME leveldb
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
  )
  if(LEVELDB_PLATFORM STREQUAL "POSIX")
    target_compile_options(leveldb-shared PRIVATE -fPIC)
  endif()
endif()

# MemEnv library
add_library(memenv STATIC ${MEMENV_SOURCES})
target_link_libraries(memenv leveldb)

# Executables
# add_executable(db_bench db/db_bench.cc)
# target_link_libraries(db_bench leveldb)

add_executable(leveldbutil db/leveldb_main.cc)
target_link_libraries(leveldbutil leveldb)

# Tests
enable_testing()

# Define test executables
set(LEVELDB_TESTS
  arena_test
  autocompact_test
  bloom_test
  c_test
  cache_test
  coding_test
  corruption_test
  crc32c_test
  db_test
  dbformat_test
  env_test
  filename_test
  filter_block_test
  issue178_test
  issue200_test
  log_test
  memenv_test
  skiplist_test
  table_test
  version_edit_test
  version_set_test
  write_batch_test
  main_test
)

# Build test libraries
add_library(testutil OBJECT ${TESTUTIL_SOURCES})
target_compile_definitions(testutil PRIVATE -DLEVELDB_PLATFORM_POSIX=1)

add_library(testharness OBJECT ${TESTHARNESS_SOURCES})
target_compile_definitions(testharness PRIVATE -DLEVELDB_PLATFORM_POSIX=1)

if (LEVELDB_BUILD_TESTS)
    # Build all test executables
    foreach(test_name ${LEVELDB_TESTS})
    if(${test_name} STREQUAL "main_test")
        add_executable(${test_name} util/${test_name}.cc)
        target_link_libraries(${test_name} leveldb)
    elseif(${test_name} STREQUAL "memenv_test")
        add_executable(${test_name} helpers/memenv/${test_name}.cc $<TARGET_OBJECTS:testharness>)
        target_link_libraries(${test_name} memenv leveldb)
    else()
        # Determine source file location
        if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/db/${test_name}.cc")
        set(test_file "db/${test_name}.cc")
        elseif(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/util/${test_name}.cc")
        set(test_file "util/${test_name}.cc")
        elseif(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/table/${test_name}.cc")
        set(test_file "table/${test_name}.cc")
        elseif(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/issues/${test_name}.cc")
        set(test_file "issues/${test_name}.cc")
        else()
        message(FATAL_ERROR "Cannot find source file for test: ${test_name}")
        endif()
        
        add_executable(${test_name} ${test_file} $<TARGET_OBJECTS:testharness>)
        target_link_libraries(${test_name} leveldb)
    endif()
    
    add_test(NAME ${test_name} COMMAND ${test_name})
    endforeach()
endif()

# Benchmarks
if(LEVELDB_BUILD_BENCHMARKS)
  # Add the Google benchmark library
  set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
  set(BENCHMARK_ENABLE_EXCEPTIONS OFF CACHE BOOL "" FORCE)
  add_subdirectory("third_party/benchmark")

  # Define test utilities if not already defined
  if(NOT TARGET testutil)
    add_library(testutil STATIC ${TESTUTIL_SOURCES})
    target_link_libraries(testutil leveldb)
  endif()

  # Helper function to create benchmark targets
  function(leveldb_benchmark bench_file)
    get_filename_component(bench_target_name "${bench_file}" NAME_WE)

    add_executable("${bench_target_name}" "")
    target_sources("${bench_target_name}"
      PRIVATE
      "util/histogram.cc"
      "util/histogram.h"
      "util/testutil.cc"
      "util/testutil.h"
      "${bench_file}"
    )
    
    target_link_libraries("${bench_target_name}" leveldb benchmark Threads::Threads)
    
    target_compile_definitions("${bench_target_name}"
      PRIVATE
      ${LEVELDB_PLATFORM_NAME}=1
    )
  endfunction(leveldb_benchmark)

  # Main LevelDB benchmarks
  #leveldb_benchmark("db/db_bench.cc")
  
  # Add other custom benchmarks if you have them
  # Example: leveldb_benchmark("benchmarks/db_index.cc")

  # SQLite benchmark
  include(CheckLibraryExists)
  check_library_exists(sqlite3 sqlite3_open "" HAVE_SQLITE3)
  
  if(HAVE_SQLITE3)
    leveldb_benchmark("doc/bench/db_bench_sqlite3.cc")
    target_link_libraries(db_bench_sqlite3 sqlite3)
  endif(HAVE_SQLITE3)

  # KyotoCabinet benchmark
  set(OLD_CMAKE_REQUIRED_LIBRARIES ${CMAKE_REQUIRED_LIBRARIES})
  list(APPEND CMAKE_REQUIRED_LIBRARIES kyotocabinet)
  include(CheckCXXSourceCompiles)
  check_cxx_source_compiles("
  #include <kcpolydb.h>
  
  int main() {
    kyotocabinet::TreeDB* db = new kyotocabinet::TreeDB();
    delete db;
    return 0;
  }
  " HAVE_KYOTOCABINET)
  set(CMAKE_REQUIRED_LIBRARIES ${OLD_CMAKE_REQUIRED_LIBRARIES})
  
  if(HAVE_KYOTOCABINET)
    leveldb_benchmark("doc/bench/db_bench_tree_db.cc")
    target_link_libraries(db_bench_tree_db kyotocabinet)
  endif(HAVE_KYOTOCABINET)
endif(LEVELDB_BUILD_BENCHMARKS)

if(LEVELDB_HTTP_SERVER)
  # Find required dependencies for HTTP server
  find_package(Threads REQUIRED)

  # Check for Boost - optional but recommended for Crow
  find_package(Boost COMPONENTS system QUIET)
  if(Boost_FOUND)
    set(CROW_USE_BOOST ON)
    message(STATUS "Boost found. Enabling Boost support for Crow.")
  else()
    set(CROW_USE_BOOST OFF)
    message(STATUS "Boost not found. Building Crow without Boost support.")
  endif()

  # OpenSSL for HTTPS support (optional)
  find_package(OpenSSL QUIET)
  if(OpenSSL_FOUND)
    set(CROW_ENABLE_SSL ON)
    message(STATUS "OpenSSL found. Enabling HTTPS support.")
  else()
    set(CROW_ENABLE_SSL OFF)
    message(STATUS "OpenSSL not found. HTTPS support disabled.")
  endif()

  # Set Crow options
  set(CROW_BUILD_EXAMPLES OFF CACHE BOOL "Build Crow examples" FORCE)
  set(CROW_BUILD_TESTS OFF CACHE BOOL "Build Crow tests" FORCE)

  # Add the http_server directory to the build
  add_subdirectory(http_server)
endif(LEVELDB_HTTP_SERVER)

# iOS specific setup
if(LEVELDB_PLATFORM STREQUAL "IOS")
  set(CMAKE_OSX_ARCHITECTURES "armv7;arm64;x86_64")
  # Additional iOS-specific settings would go here
endif()

# Installation
install(TARGETS leveldb memenv
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

if(BUILD_SHARED_LIBS)
  install(TARGETS leveldb-shared
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  )
endif()

install(DIRECTORY include/leveldb
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
  FILES_MATCHING PATTERN "*.h"
)