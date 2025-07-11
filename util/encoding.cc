#include "util/encoding.h"
#include "util/coding.h"
#include "leveldb/slice.h"
#include <cstring>

namespace leveldb {

// Encode signed integers for proper lexicographic ordering
// Uses LevelDB's native coding with sign bit handling
std::string IntEncoder::EncodeInt32(int32_t value) {
    std::string result;
    // Convert signed to unsigned with proper ordering
    uint32_t encoded;
    if (value < 0) {
        // For negative numbers: flip all bits and add 1
        // This ensures proper lexicographic ordering
        encoded = ~static_cast<uint32_t>(-value) + 1;
    } else {
        // For positive numbers: add offset to make them sort after negatives
        encoded = static_cast<uint32_t>(value) + 0x80000000;
    }
    PutFixed32(&result, encoded);
    return result;
}

std::string IntEncoder::EncodeInt64(int64_t value) {
    std::string result;
    // Convert signed to unsigned with proper ordering
    uint64_t encoded;
    if (value < 0) {
        // For negative numbers: flip all bits and add 1
        encoded = ~static_cast<uint64_t>(-value) + 1;
    } else {
        // For positive numbers: add offset to make them sort after negatives
        encoded = static_cast<uint64_t>(value) + 0x8000000000000000ULL;
    }
    PutFixed64(&result, encoded);
    return result;
}

std::string IntEncoder::EncodeUInt32(uint32_t value) {
    std::string result;
    PutFixed32(&result, value);
    return result;
}

std::string IntEncoder::EncodeUInt64(uint64_t value) {
    std::string result;
    PutFixed64(&result, value);
    return result;
}

std::string IntEncoder::EncodeVarInt(int64_t value) {
    std::string result;
    // For signed varint, we need to handle sign bit
    if (value < 0) {
        // Encode negative as positive with sign bit
        PutVarint64(&result, static_cast<uint64_t>(-value) * 2 + 1);
    } else {
        // Encode positive as even number
        PutVarint64(&result, static_cast<uint64_t>(value) * 2);
    }
    return result;
}

// Decode functions
bool IntEncoder::DecodeInt32(const Slice& input, int32_t* value) {
    if (input.size() != 4) return false;
    
    uint32_t encoded = DecodeFixed32(input.data());
    if (encoded & 0x80000000) {
        // Positive number
        *value = static_cast<int32_t>(encoded - 0x80000000);
    } else {
        // Negative number
        *value = -static_cast<int32_t>((~encoded) + 1);
    }
    return true;
}

bool IntEncoder::DecodeInt64(const Slice& input, int64_t* value) {
    if (input.size() != 8) return false;
    
    uint64_t encoded = DecodeFixed64(input.data());
    if (encoded & 0x8000000000000000ULL) {
        // Positive number
        *value = static_cast<int64_t>(encoded - 0x8000000000000000ULL);
    } else {
        // Negative number
        *value = -static_cast<int64_t>((~encoded) + 1);
    }
    return true;
}

bool IntEncoder::DecodeUInt32(const Slice& input, uint32_t* value) {
    if (input.size() != 4) return false;
    *value = DecodeFixed32(input.data());
    return true;
}

bool IntEncoder::DecodeUInt64(const Slice& input, uint64_t* value) {
    if (input.size() != 8) return false;
    *value = DecodeFixed64(input.data());
    return true;
}

bool IntEncoder::DecodeVarInt(const Slice& input, int64_t* value) {
    uint64_t encoded;
    Slice temp = input;
    if (!GetVarint64(&temp, &encoded)) return false;
    
    if (encoded & 1) {
        // Negative number
        *value = -static_cast<int64_t>(encoded / 2);
    } else {
        // Positive number
        *value = static_cast<int64_t>(encoded / 2);
    }
    return true;
}

// Check if data is encoded as integer
bool IntEncoder::IsEncodedInt(const Slice& data) {
    // Check for fixed-length encodings (4 or 8 bytes)
    if (data.size() == 4 || data.size() == 8) {
        return true;
    }
    
    // Check for varint encoding (at least 1 byte)
    if (data.size() >= 1) {
        // Try to decode as varint to see if it's valid
        Slice temp = data;
        uint64_t dummy;
        return GetVarint64(&temp, &dummy);
    }
    
    return false;
}

// Get the type of encoded integer
IntEncoder::EncodedType IntEncoder::GetEncodedType(const Slice& data) {
    if (data.size() == 4) {
        return ENCODED_INT32;
    } else if (data.size() == 8) {
        return ENCODED_INT64;
    } else if (data.size() >= 1) {
        // Check if it's a varint
        Slice temp = data;
        uint64_t dummy;
        if (GetVarint64(&temp, &dummy)) {
            return ENCODED_VARINT;
        }
    }
    return ENCODED_UNKNOWN;
}

// String encoding using LevelDB's native encoding
std::string StringEncoder::Encode(const std::string& value) {
    std::string result;
    PutLengthPrefixedSlice(&result, Slice(value));
    return result;
}

bool StringEncoder::Decode(const Slice& input, std::string* value) {
    Slice result;
    Slice temp = input;
    if (GetLengthPrefixedSlice(&temp, &result)) {
        *value = result.ToString();
        return true;
    }
    return false;
}

// Boolean encoding - simple 1-byte encoding
std::string BoolEncoder::Encode(bool value) {
    std::string result;
    result.push_back(value ? '\x01' : '\x00');
    return result;
}

bool BoolEncoder::Decode(const Slice& input, bool* value) {
    if (input.size() != 1) return false;
    *value = (input.data()[0] != '\x00');
    return true;
}

// Combined encoder for mixed types
std::string Encoder::EncodeInt32(int32_t value) {
    return IntEncoder::EncodeInt32(value);
}

std::string Encoder::EncodeInt64(int64_t value) {
    return IntEncoder::EncodeInt64(value);
}

std::string Encoder::EncodeString(const std::string& value) {
    return StringEncoder::Encode(value);
}

std::string Encoder::EncodeBool(bool value) {
    return BoolEncoder::Encode(value);
}

std::string Encoder::EncodeVarInt(int64_t value) {
    return IntEncoder::EncodeVarInt(value);
}

bool Encoder::DecodeInt32(const Slice& input, int32_t* value) {
    return IntEncoder::DecodeInt32(input, value);
}

bool Encoder::DecodeInt64(const Slice& input, int64_t* value) {
    return IntEncoder::DecodeInt64(input, value);
}

bool Encoder::DecodeString(const Slice& input, std::string* value) {
    return StringEncoder::Decode(input, value);
}

bool Encoder::DecodeVarInt(const Slice& input, int64_t* value) {
    return IntEncoder::DecodeVarInt(input, value);
}

bool Encoder::DecodeBool(const Slice& input, bool* value) {
    return BoolEncoder::Decode(input, value);
}

} // namespace leveldb 