#include "iceberg/tea_scan_hashers.h"

#include "iceberg/tea_scan.h"

namespace iceberg::ice_tea {

size_t PositionalDeleteInfoHasher::operator()(const PositionalDeleteInfo& pos_delete) const {
  return path_hasher(pos_delete.path);
}

size_t EqualityDeleteInfoHasher::operator()(const EqualityDeleteInfo& pos_delete) const {
  return path_hasher(pos_delete.path);
}

size_t PartitionHasher::operator()(const ScanMetadata::Partition& partition) const {
  size_t hash = 0;
  for (const auto& layer : partition) {
    hash ^= hasher(layer);
  }
  return hash;
}

size_t LayerHasher::operator()(const ScanMetadata::Layer& layer) const {
  size_t hash = 0;
  for (const auto& data_entry : layer.data_entries_) {
    hash ^= hasher(data_entry.path);
    for (const auto& segment : data_entry.parts) {
      hash ^= segment_hasher(segment);
    }
  }
  for (const auto& data_entry : layer.equality_delete_entries_) {
    hash ^= hasher(data_entry.path);
  }
  for (const auto& data_entry : layer.positional_delete_entries_) {
    hash ^= hasher(data_entry.path);
  }
  return hash;
}

size_t DataEntryHasher::operator()(const DataEntry& data_entry) const {
  size_t hash = hasher(data_entry.path);

  for (const auto& segment : data_entry.parts) {
    hash ^= segment_hasher(segment);
  }
  return hash;
}

}  // namespace iceberg::ice_tea
