#pragma once

#include <iceberg/puffin.h>

#include <span>
#include <roaring.hh>

namespace iceberg {

namespace properties_names {
constexpr std::string_view kCardinality = "cardinality";
constexpr std::string_view kReferencedDataFile = "referenced-data-file";
}  // namespace properties_names
class DeletionVector {
  using BlobMetadata = PuffinFile::Footer::BlobMetadata;

 public:
  DeletionVector(const BlobMetadata& blob_metadata, const std::string& blob);
  DeletionVector(const BlobMetadata& blob_metadata, roaring::Roaring64Map bitmap);

  std::string GetBlob() const;

  const std::map<std::string, std::string>& GetProperties() const;
  const std::vector<int32_t>& GetFields() const;

  bool Add(uint64_t x);
  bool Remove(uint64_t x);
  bool Contains(uint64_t x) const;

  // Optimized for better performance
  void AddMany(std::span<uint64_t> vals);
  void AddMany(const std::vector<uint64_t>& vals);
  // Adds range [l, r] to rows to be deleted
  void AddRange(uint64_t l, uint64_t r);
  // Remove range [l, r] from rows to be deleted
  void RemoveRange(uint64_t l, uint64_t r);

  void Clear();
  bool Empty() const;

  std::optional<uint64_t> Max() const;  // returns nullopt if empty
  std::optional<uint64_t> Min() const;  // returns nullopt if empty

  // returns number of rows to be deleted in [l; r]
  uint64_t Cardinality(uint64_t l = 0, uint64_t r = UINT64_MAX) const;
  // returns vector of rows to be deleted from [l; r]
  std::vector<uint64_t> GetElems(uint64_t l, uint64_t r) const;

  static constexpr std::string_view kMagicSequence = "\xd1\xd3\x39\x64";
  static_assert(kMagicSequence.size() == 4);
  static constexpr std::string_view kBlobType = "deletion-vector-v1";
  static constexpr uint32_t kMagicSequenceOffset = sizeof(uint32_t);
  static constexpr uint32_t kDataOffset = kMagicSequenceOffset + kMagicSequence.size();

 private:
  mutable roaring::Roaring64Map bitmap_;
  mutable std::map<std::string, std::string> properties_;
  std::vector<int32_t> fields_;
};
}  // namespace iceberg
