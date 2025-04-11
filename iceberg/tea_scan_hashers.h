#pragma once

#include "iceberg/tea_scan.h"

namespace iceberg::ice_tea {

struct SegmentHasher {
  size_t operator()(const DataEntry::Segment& segment) const {
    return (segment.offset ^ segment.length) + segment.length;
  }
};

struct DataEntryHasher {
  size_t operator()(const DataEntry& data_entry) const;

 private:
  std::hash<std::string> hasher;
  SegmentHasher segment_hasher;
};

struct PositionalDeleteInfoHasher {
  size_t operator()(const PositionalDeleteInfo& pos_delete) const;

 private:
  std::hash<std::string> path_hasher;
};

struct EqualityDeleteInfoHasher {
  size_t operator()(const EqualityDeleteInfo& pos_delete) const;

 private:
  std::hash<std::string> path_hasher;
};

struct LayerHasher {
  size_t operator()(const ScanMetadata::Layer& layer) const;

 private:
  std::hash<std::string> hasher;
  SegmentHasher segment_hasher;
};

struct PartitionHasher {
  size_t operator()(const ScanMetadata::Partition& partition) const;

 private:
  LayerHasher hasher;
};

}  // namespace iceberg::ice_tea
