#ifndef STORAGE_LEVELDB_UTIL_ENCODING_H_
#define STORAGE_LEVELDB_UTIL_ENCODING_H_

#include <string>
#include <cstdint>
#include <vector>
#include <map>
#include "leveldb/slice.h"

namespace leveldb {

// Integer encoding using LevelDB's native coding functions
class IntEncoder {
public:
    enum EncodedType {
        ENCODED_UNKNOWN,
        ENCODED_INT32,
        ENCODED_INT64,
        ENCODED_UINT32,
        ENCODED_UINT64,
        ENCODED_VARINT
    };

    // Encode signed integers for proper lexicographic ordering
    static std::string EncodeInt32(int32_t value);
    static std::string EncodeInt64(int64_t value);
    static std::string EncodeUInt32(uint32_t value);
    static std::string EncodeUInt64(uint64_t value);
    static std::string EncodeVarInt(int64_t value);

    // Decode functions
    static bool DecodeInt32(const Slice& input, int32_t* value);
    static bool DecodeInt64(const Slice& input, int64_t* value);
    static bool DecodeUInt32(const Slice& input, uint32_t* value);
    static bool DecodeUInt64(const Slice& input, uint64_t* value);
    static bool DecodeVarInt(const Slice& input, int64_t* value);

    // Utility functions
    static bool IsEncodedInt(const Slice& data);
    static EncodedType GetEncodedType(const Slice& data);
};

// String encoding using LevelDB's native encoding
class StringEncoder {
public:
    static std::string Encode(const std::string& value);
    static bool Decode(const Slice& input, std::string* value);
};

// Boolean encoding
class BoolEncoder {
public:
    static std::string Encode(bool value);
    static bool Decode(const Slice& input, bool* value);
};

// Combined encoder for mixed types
class Encoder {
public:
    // Integer encoding
    static std::string EncodeInt32(int32_t value);
    static std::string EncodeInt64(int64_t value);
    static std::string EncodeVarInt(int64_t value);
    
    // String encoding
    static std::string EncodeString(const std::string& value);
    
    // Boolean encoding
    static std::string EncodeBool(bool value);
    
    // Decoding
    static bool DecodeInt32(const Slice& input, int32_t* value);
    static bool DecodeInt64(const Slice& input, int64_t* value);
    static bool DecodeString(const Slice& input, std::string* value);
    static bool DecodeVarInt(const Slice& input, int64_t* value);
    static bool DecodeBool(const Slice& input, bool* value);
};

// Helper for creating composite keys with encoded integers
std::string CreateCompositeKey(const std::string& prefix, int32_t value1, 
                              const std::string& separator, int32_t value2);

} // namespace leveldb


#endif // STORAGE_LEVELDB_UTIL_ENCODING_H_ 