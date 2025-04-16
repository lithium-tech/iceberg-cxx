#pragma once

#include <string>
#include <utility>

namespace iceberg {

using FieldId = int32_t;
using LayerId = int32_t;
using PartitionId = int32_t;
using RowPosition = int64_t;

class PartitionLayer {
 public:
  PartitionLayer(PartitionId partition, LayerId layer) : partition_(partition), layer_(layer) {}

  PartitionId GetPartition() const { return partition_; }
  LayerId GetLayer() const { return layer_; }
  PartitionLayer GetPartitionLayer() const { return *this; }

  std::string DebugString() const {
    return "(partition = " + std::to_string(partition_) + ", layer = " + std::to_string(layer_) + ")";
  }

  bool operator==(const PartitionLayer& other) const = default;

 protected:
  PartitionId partition_;
  LayerId layer_;
};

class PartitionLayerFile : public PartitionLayer {
 public:
  PartitionLayerFile(PartitionLayer partition_layer, std::string path)
      : PartitionLayer(partition_layer), path_(std::move(path)) {}

  const std::string& GetPath() const { return path_; }
  const PartitionLayerFile& GetPartitionLayerFile() const { return *this; }

  bool operator==(const PartitionLayerFile& other) const = default;

 protected:
  std::string path_;
};

struct PartitionLayerFilePosition : public PartitionLayerFile {
 public:
  PartitionLayerFilePosition(PartitionLayerFile partition_layer_file, RowPosition row_position)
      : PartitionLayerFile(std::move(partition_layer_file)), row_position_(row_position) {}

  const PartitionLayerFilePosition& GetPartitionLayerFilePosition() const { return *this; }
  RowPosition GetRowPosition() const { return row_position_; }

  bool operator==(const PartitionLayerFilePosition& other) const = default;

 protected:
  RowPosition row_position_;
};

struct ColumnInfo {
  FieldId field_id;
  std::string result_name;

  ColumnInfo() = delete;
  ColumnInfo(FieldId f, std::string n) : field_id(f), result_name(std::move(n)) {}
};

}  // namespace iceberg
