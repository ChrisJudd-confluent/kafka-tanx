#pragma once
#include <vector>
#include <string>
#include <cstdint>
#include <cstring>

// =============================================================================
// AvroWriter — binary Avro encoder (zigzag varints, IEEE 754 floats, etc.)
//
// Usage:
//   AvroWriter w;
//   w.WriteString("hello");
//   w.WriteInt(42);
//   auto bytes = w.Data();
// =============================================================================
class AvroWriter {
public:
    void WriteNull() {}

    void WriteBool(bool v) {
        buf_.push_back(v ? 0x01 : 0x00);
    }

    void WriteInt(int32_t v) { WriteLong((int64_t)v); }

    void WriteLong(int64_t v) {
        // zigzag encode, then 7-bit variable-length encode
        uint64_t z = ((uint64_t)v << 1) ^ (uint64_t)(v >> 63);
        while (z & ~0x7FULL) {
            buf_.push_back((uint8_t)((z & 0x7F) | 0x80));
            z >>= 7;
        }
        buf_.push_back((uint8_t)z);
    }

    void WriteFloat(float v) {
        uint8_t bytes[4];
        std::memcpy(bytes, &v, 4);
        for (auto b : bytes) buf_.push_back(b);
    }

    void WriteString(const std::string& s) {
        WriteLong((int64_t)s.size());
        for (unsigned char c : s) buf_.push_back(c);
    }

    void WriteBytes(const std::vector<uint8_t>& b) {
        WriteLong((int64_t)b.size());
        buf_.insert(buf_.end(), b.begin(), b.end());
    }

    void WriteEnum(int32_t index) { WriteLong((int64_t)index); }

    // Nullable string: union ["null", "string"].
    // index 0 = null branch, index 1 = string branch.
    void WriteNullableString(const std::string& s, bool isNull) {
        if (isNull) { WriteLong(0); }
        else        { WriteLong(1); WriteString(s); }
    }

    const std::vector<uint8_t>& Data() const { return buf_; }
    void Clear() { buf_.clear(); }

private:
    std::vector<uint8_t> buf_;
};

// =============================================================================
// AvroReader — binary Avro decoder
//
// Usage:
//   AvroReader r(data, len);
//   std::string s = r.ReadString();
//   int32_t n     = r.ReadInt();
//   if (!r.Ok()) { /* truncated or corrupt */ }
// =============================================================================
class AvroReader {
public:
    AvroReader(const uint8_t* data, size_t len)
        : data_(data), len_(len), pos_(0), error_(false) {}

    bool ReadBool() { return ReadByte() != 0x00; }

    int32_t ReadInt() { return (int32_t)ReadLong(); }

    int64_t ReadLong() {
        uint64_t z = 0;
        int shift = 0;
        uint8_t b;
        do {
            b = ReadByte();
            z |= (uint64_t)(b & 0x7F) << shift;
            shift += 7;
        } while (b & 0x80);
        return (int64_t)((z >> 1) ^ (uint64_t)(-(int64_t)(z & 1)));
    }

    float ReadFloat() {
        uint8_t bytes[4];
        for (int i = 0; i < 4; i++) bytes[i] = ReadByte();
        float v;
        std::memcpy(&v, bytes, 4);
        return v;
    }

    std::string ReadString() {
        int64_t len = ReadLong();
        if (len < 0 || len > 1024 * 1024) { error_ = true; return ""; }
        std::string s;
        s.reserve((size_t)len);
        for (int64_t i = 0; i < len; i++) s += (char)ReadByte();
        return s;
    }

    std::vector<uint8_t> ReadBytes() {
        int64_t len = ReadLong();
        if (len < 0 || len > 4 * 1024 * 1024) { error_ = true; return {}; }
        std::vector<uint8_t> b;
        b.reserve((size_t)len);
        for (int64_t i = 0; i < len; i++) b.push_back(ReadByte());
        return b;
    }

    int32_t ReadEnum() { return (int32_t)ReadLong(); }

    // Nullable string: reads union index then optionally the string value.
    std::string ReadNullableString(bool& isNull) {
        int64_t idx = ReadLong();
        if (idx == 0) { isNull = true;  return ""; }
        isNull = false;
        return ReadString();
    }

    bool Ok() const { return !error_ && pos_ <= len_; }

private:
    uint8_t ReadByte() {
        if (pos_ >= len_) { error_ = true; return 0; }
        return data_[pos_++];
    }

    const uint8_t* data_;
    size_t         len_;
    size_t         pos_;
    bool           error_;
};

// =============================================================================
// AvroWire — Confluent Schema Registry wire format helpers
//
// Every message on a Confluent Cloud topic is framed as:
//   [0x00][schema_id: 4 bytes big-endian][avro binary payload...]
//
// Flink and ksqlDB automatically strip the 5-byte header when deserialising
// using the registered schema. Hand-rolling this is the only Avro-library
// dependency we need to avoid pulling in avro-cpp + libserdes.
// =============================================================================
namespace AvroWire {

inline std::vector<uint8_t> Wrap(int32_t schemaId,
                                  const std::vector<uint8_t>& payload) {
    std::vector<uint8_t> out;
    out.reserve(5 + payload.size());
    out.push_back(0x00);
    out.push_back((uint8_t)((schemaId >> 24) & 0xFF));
    out.push_back((uint8_t)((schemaId >> 16) & 0xFF));
    out.push_back((uint8_t)((schemaId >>  8) & 0xFF));
    out.push_back((uint8_t)( schemaId        & 0xFF));
    out.insert(out.end(), payload.begin(), payload.end());
    return out;
}

// Returns false if the magic byte is missing or the buffer is too short.
inline bool Unwrap(const uint8_t* data, size_t len,
                   int32_t& schemaIdOut,
                   const uint8_t*& payloadOut, size_t& payloadLen) {
    if (len < 5 || data[0] != 0x00) return false;
    schemaIdOut = ((int32_t)data[1] << 24) | ((int32_t)data[2] << 16)
                | ((int32_t)data[3] <<  8) |  (int32_t)data[4];
    payloadOut  = data + 5;
    payloadLen  = len  - 5;
    return true;
}

} // namespace AvroWire
