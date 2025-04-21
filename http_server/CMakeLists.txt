# HTTP Server for LevelDB

# Add Crow as a header-only library
add_library(crow INTERFACE)
target_include_directories(crow INTERFACE
    ${CMAKE_SOURCE_DIR}/third_party/crow/include
)
set(RAPIDJSON_INCLUDE_DIR "${CMAKE_SOURCE_DIR}/include/rapidjson/include")

# Build the HTTP server executable
add_executable(leveldb_http_server http_server.cc)

# Include RapidJSON headers
target_include_directories(leveldb_http_server PRIVATE
    ${RAPIDJSON_INCLUDE_DIR}
)

target_link_libraries(leveldb_http_server PRIVATE
    leveldb
    crow
    Threads::Threads
)

if(CROW_USE_BOOST)
    target_link_libraries(leveldb_http_server PRIVATE ${Boost_LIBRARIES})
    target_compile_definitions(leveldb_http_server PRIVATE CROW_USE_BOOST)
endif()

if(CROW_ENABLE_SSL)
    target_link_libraries(leveldb_http_server PRIVATE OpenSSL::SSL OpenSSL::Crypto)
    target_compile_definitions(leveldb_http_server PRIVATE CROW_ENABLE_SSL)
endif()

# Set compiler options
target_compile_features(leveldb_http_server PRIVATE cxx_std_11)