#include "iceberg/deletion_vector.h"

// clang-format off
#include <vendor/zlib-ng/zconf.h.in>
#include <vendor/zlib-ng/zlib.h>
// clang-format on

namespace iceberg {
namespace {
void Ensure(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

std::string UItoLittleEndianBytes(uint32_t value) {
  std::string res;
  res.resize(sizeof(uint32_t));
  memcpy(res.data(), &value, sizeof(uint32_t));
  std::reverse(res.begin(), res.end());
  return res;
}

uint32_t GetLen(const std::string& data) {
  uint32_t len;
  std::string str = data.substr(0, sizeof(uint32_t));
  std::reverse(str.begin(), str.end());
  memcpy(&len, str.data(), str.size());
  Ensure(len + 2 * sizeof(uint32_t) == data.size(), "deletion vector blob's length doesn't match expected: expected " +
                                                        std::to_string(len + 2 * sizeof(uint32_t)) +
                                                        ", but actual is " + std::to_string(data.size()));
  return len;
}

void ValidateMagicSequence(const std::string& data) {
  Ensure(data.substr(DeletionVector::kMagicSequenceOffset, DeletionVector::kMagicSequence.size()) ==
             DeletionVector::kMagicSequence,
         "blob data contains wrong magic sequence: expected " + std::string(DeletionVector::kMagicSequence) +
             ", but actual is " +
             data.substr(DeletionVector::kMagicSequenceOffset, DeletionVector::kMagicSequence.size()));
}

uint32_t GetCheckSum(const std::string& data) {
  uint32_t checksum;
  std::string str = data.substr(data.size() - sizeof(uint32_t), sizeof(uint32_t));
  std::reverse(str.begin(), str.end());
  memcpy(&checksum, str.data(), str.size());
  return checksum;
}

using BlobMetadata = PuffinFile::Footer::BlobMetadata;

}  // namespace

DeletionVector::DeletionVector(const BlobMetadata& blob_metadata, const std::string& blob)
    : properties_(blob_metadata.properties), fields_(blob_metadata.fields) {
  Ensure(blob_metadata.type == kBlobType,
         "Invalid blob type: expected " + std::string(kBlobType) + ", found " + blob_metadata.type);
  Ensure(blob_metadata.properties.contains(std::string(properties_names::kReferencedDataFile)),
         std::string(properties_names::kReferencedDataFile) + " not found");
  Ensure(!blob_metadata.properties.contains("compression-codec"),
         "deletion vector blob must omit compession-codec in properties");
  Ensure(!blob_metadata.compression_codec.has_value(), "deletion vector blob must omit compression-codec in metadata");
  Ensure(blob_metadata.length == blob.size(), "deletion vector blob's length (" + std::to_string(blob_metadata.length) +
                                                  ") doesn't match metadata length (" + std::to_string(blob.size()) +
                                                  ")");
  Ensure(blob.size() >= 2 * sizeof(uint32_t) + DeletionVector::kMagicSequence.size(),
         "deletion vector blob is too small");
  auto len = GetLen(blob);
  ValidateMagicSequence(blob);
  try {
    bitmap_ = roaring::Roaring64Map::readSafe(&blob[kDataOffset], len - kMagicSequence.size());
  } catch (const std::exception& e) {
    throw std::runtime_error("Failed to deserialize deletion vector: " + std::string(e.what()));
  }
  auto it = blob_metadata.properties.find(std::string(properties_names::kCardinality));
  Ensure(it != blob_metadata.properties.end(), std::string(properties_names::kCardinality) + " not found");
  Ensure(std::stoull(it->second) == Cardinality(),
         "cardinality doesn't match: expected" + it->second + ", actual is " + std::to_string(bitmap_.cardinality()));
  uint32_t real_checksum = crc32(0, reinterpret_cast<const unsigned char*>(&blob[kMagicSequenceOffset]), len);
  uint32_t expected_checksum = GetCheckSum(blob);
  Ensure(real_checksum == expected_checksum, "crc32 checksum doesn't match: expected " +
                                                 std::to_string(expected_checksum) + ", actual is " +
                                                 std::to_string(real_checksum));
}

DeletionVector::DeletionVector(const BlobMetadata& blob_metadata, roaring::Roaring64Map bitmap)
    : properties_(blob_metadata.properties), fields_(blob_metadata.fields), bitmap_(std::move(bitmap)) {}

std::string DeletionVector::GetBlob() const {
  std::string res;
  size_t len = kMagicSequence.size() + bitmap_.getSizeInBytes();
  res.reserve(2 * sizeof(uint32_t) + len);
  if (len > UINT32_MAX) {
    bitmap_.shrinkToFit();
    len = kMagicSequence.size() + bitmap_.getSizeInBytes();
    Ensure(len <= UINT32_MAX, "deletion vector is too big to be serialized");
  }

  res += UItoLittleEndianBytes(len);
  res += kMagicSequence;
  res.resize(res.size() + bitmap_.getSizeInBytes());
  bitmap_.write(&res[kDataOffset]);
  res += UItoLittleEndianBytes(crc32(0, reinterpret_cast<const unsigned char*>(&res[kMagicSequenceOffset]), len));
  return res;
}

const std::map<std::string, std::string>& DeletionVector::GetProperties() const {
  properties_[std::string(properties_names::kCardinality)] = std::to_string(Cardinality());
  return properties_;
}

const std::vector<int32_t>& DeletionVector::GetFields() const { return fields_; }

bool DeletionVector::Add(uint64_t x) { return bitmap_.addChecked(x); }

bool DeletionVector::Remove(uint64_t x) { return bitmap_.removeChecked(x); }

bool DeletionVector::Contains(uint64_t x) const { return bitmap_.contains(x); }

void DeletionVector::AddMany(std::span<uint64_t> vals) { bitmap_.addMany(vals.size(), vals.data()); }

void DeletionVector::AddMany(const std::vector<uint64_t>& vals) { bitmap_.addMany(vals.size(), vals.data()); }

void DeletionVector::AddRange(uint64_t l, uint64_t r) { bitmap_.addRangeClosed(l, r); }

void DeletionVector::RemoveRange(uint64_t l, uint64_t r) { bitmap_.removeRangeClosed(l, r); }

void DeletionVector::Clear() { bitmap_.clear(); }

bool DeletionVector::Empty() const { return bitmap_.isEmpty(); }

std::optional<uint64_t> DeletionVector::Max() const {
  if (Empty()) {
    return std::nullopt;
  }
  return bitmap_.maximum();
}

std::optional<uint64_t> DeletionVector::Min() const {
  if (Empty()) {
    return std::nullopt;
  }
  return bitmap_.minimum();
}

uint64_t DeletionVector::Cardinality(uint64_t l, uint64_t r) const {
  return bitmap_.rank(r) - bitmap_.rank(l) + bitmap_.contains(l);
}

std::vector<uint64_t> DeletionVector::GetElems(uint64_t l, uint64_t r) const {
  auto it = bitmap_.begin();
  it.move(l);
  std::vector<uint64_t> res;
  res.reserve(Cardinality(l, r));
  while (it != bitmap_.end() && *it <= r) {
    res.push_back(*it);
    ++it;
  }
  return res;
}
}  // namespace iceberg
