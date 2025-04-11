#include "iceberg/experimental_representations.h"

#include <stdexcept>

#include "iceberg/schema.h"
#include "iceberg/tea_scan.h"
#include "iceberg/tea_scan_hashers.h"

namespace iceberg::experimental {

namespace {

using IcebergFile = std::variant<ice_tea::PositionalDeleteInfo, ice_tea::DataEntry, ice_tea::EqualityDeleteInfo>;

std::vector<IcebergFile> MakeFlatFilesList(const ice_tea::ScanMetadata::Partition& partition) {
  std::vector<IcebergFile> result;

  for (int i = partition.size(); i > 0; --i) {
    const auto& layer = partition[i - 1];
    for (const auto& data_entry : layer.data_entries_) {
      result.push_back(data_entry);
    }
    for (const auto& positional_delete : layer.positional_delete_entries_) {
      result.push_back(positional_delete);
    }
    for (const auto& equality_delete : layer.equality_delete_entries_) {
      result.push_back(equality_delete);
    }
  }
  return result;
}

}  // namespace

bool ValidateSegments(const std::vector<ice_tea::DataEntry::Segment>& segments) {
  for (size_t i = 1; i < segments.size(); ++i) {
    if (segments[i].offset < segments[i - 1].offset) {
      return false;
    }
  }
  return true;
}

std::vector<ice_tea::DataEntry::Segment> UniteSegments(const std::vector<ice_tea::DataEntry::Segment>& segments) {
  if (!ValidateSegments(segments)) {
    throw std::runtime_error("Segments should be sorted");
  }

  std::vector<ice_tea::DataEntry::Segment> result;

  std::optional<ice_tea::DataEntry::Segment> back_segment;
  for (const auto& part : segments) {
    if (!back_segment) {
      back_segment = part;
      continue;
    }

    if (back_segment->length == 0) {
      break;
    }

    if (back_segment->offset + back_segment->length >= part.offset) {
      if (part.length != 0) {
        back_segment->length =
            std::max(back_segment->offset + back_segment->length, part.offset + part.length) - back_segment->offset;
      } else {
        back_segment->length = 0;
      }
    } else {
      result.push_back(*back_segment);
      back_segment = part;
    }
  }

  if (back_segment) {
    result.push_back(*back_segment);
  }
  return result;
}

struct ScanMetadataRepresentation {
  Schema schema;
  std::unordered_map<std::string,
                     std::unordered_set<ice_tea::PositionalDeleteInfo, ice_tea::PositionalDeleteInfoHasher>>
      positional_deletes_per_file;
  std::unordered_map<std::string, std::unordered_set<ice_tea::EqualityDeleteInfo, ice_tea::EqualityDeleteInfoHasher>>
      equality_deletes_per_file;
  std::unordered_map<std::string, ice_tea::DataEntry> data_entries_per_file;

  bool operator==(const ScanMetadataRepresentation&) const = default;
};

ScanMetadataRepresentation MakeRepresentation(const ice_tea::ScanMetadata& scan_meta) {
  std::unordered_map<std::string,
                     std::unordered_set<ice_tea::PositionalDeleteInfo, ice_tea::PositionalDeleteInfoHasher>>
      data_file_to_positional_deletes;
  std::unordered_map<std::string, std::unordered_set<ice_tea::EqualityDeleteInfo, ice_tea::EqualityDeleteInfoHasher>>
      data_file_to_equality_deletes;
  std::unordered_map<std::string, ice_tea::DataEntry> data_file_segments;

  for (const auto& partition : scan_meta.partitions) {
    auto plan = MakeFlatFilesList(partition);
    std::unordered_set<ice_tea::PositionalDeleteInfo, ice_tea::PositionalDeleteInfoHasher> current_positional_deletes;
    std::unordered_set<ice_tea::EqualityDeleteInfo, ice_tea::EqualityDeleteInfoHasher> current_equality_deletes;

    for (const auto& file : plan) {
      if (std::holds_alternative<ice_tea::PositionalDeleteInfo>(file)) {
        auto positional_file = std::get<ice_tea::PositionalDeleteInfo>(file);
        current_positional_deletes.insert(positional_file);
      } else if (std::holds_alternative<ice_tea::DataEntry>(file)) {
        auto file_name = std::get<ice_tea::DataEntry>(file).path;
        for (const auto& delete_file : current_positional_deletes) {
          data_file_to_positional_deletes[file_name].insert(delete_file);
        }

        for (const auto& delete_file : current_equality_deletes) {
          data_file_to_equality_deletes[file_name].insert(delete_file);
        }

      } else if (std::holds_alternative<ice_tea::EqualityDeleteInfo>(file)) {
        auto eq_file = std::get<ice_tea::EqualityDeleteInfo>(file);
        current_equality_deletes.insert(eq_file);
      }
    }

    for (const auto& layer : partition) {
      for (const auto& data_entry : layer.data_entries_) {
        auto it = data_file_segments.find(data_entry.path);
        if (it != data_file_segments.end()) {
          it->second += data_entry;
        } else {
          data_file_segments.emplace(data_entry.path, data_entry);
        }
      }
    }
  }

  auto schema = scan_meta.schema ? *scan_meta.schema : iceberg::Schema(0, {});
  return ScanMetadataRepresentation{.schema = schema,
                                    .positional_deletes_per_file = data_file_to_positional_deletes,
                                    .equality_deletes_per_file = data_file_to_equality_deletes,
                                    .data_entries_per_file = data_file_segments};
}

std::tuple<std::string, std::unordered_set<ice_tea::DataEntry::Segment, ice_tea::SegmentHasher>> MakeRepresentation(
    const ice_tea::DataEntry& data_entry) {
  std::unordered_set<ice_tea::DataEntry::Segment, ice_tea::SegmentHasher> set_parts;
  for (const auto& segment : UniteSegments(data_entry.parts)) {
    set_parts.insert(segment);
  }
  return std::tie(data_entry.path, set_parts);
}

bool AreDataEntriesEqual(const iceberg::ice_tea::DataEntry& lhs, const iceberg::ice_tea::DataEntry& rhs) {
  return MakeRepresentation(lhs) == MakeRepresentation(rhs);
}

bool AreScanMetadataEqual(const iceberg::ice_tea::ScanMetadata& lhs, const iceberg::ice_tea::ScanMetadata& rhs) {
  return MakeRepresentation(lhs) == MakeRepresentation(rhs);
}

}  // namespace iceberg::experimental
