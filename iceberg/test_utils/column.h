#pragma once

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/filesystem/filesystem.h"
#include "iceberg/test_utils/optional_vector.h"
#include "parquet/schema.h"
#include "parquet/types.h"

namespace iceberg {

struct ParquetInfo {
  std::string name;
  parquet::Type::type physical_type;
  std::shared_ptr<const parquet::LogicalType> logical_type;
  int field_id = -1;
  int length = -1;
  parquet::Repetition::type repetition = parquet::Repetition::OPTIONAL;

  parquet::schema::NodePtr MakeField() const;
};

struct ArrayContainer;

using ParquetColumnData =
    std::variant<OptionalVector<bool>, OptionalVector<int32_t>, OptionalVector<int64_t>, OptionalVector<float>,
                 OptionalVector<double>, OptionalVector<parquet::FixedLenByteArray>, OptionalVector<parquet::ByteArray>,
                 ArrayContainer>;

struct ArrayContainer {
  std::vector<ParquetColumnData> arrays;

  size_t size() const { return arrays.size(); }
};

uint32_t GetSize(const ParquetColumnData& data);

struct ParquetColumn {
  ParquetInfo info;
  ParquetColumnData data;

  size_t Size() const { return GetSize(data); }
};

struct Table {
  std::vector<ParquetColumn> columns;
  std::vector<size_t> row_group_sizes;
};

ParquetColumn MakeBoolColumn(const std::string& name, int field_id, const OptionalVector<bool>& data);
ParquetColumn MakeInt16Column(const std::string& name, int field_id, const OptionalVector<int32_t>& data);
ParquetColumn MakeInt32Column(const std::string& name, int field_id, const OptionalVector<int32_t>& data);
ParquetColumn MakeInt32ArrayColumn(const std::string& name, int field_id, const ArrayContainer& arrays);
ParquetColumn MakeInt64Column(const std::string& name, int field_id, const OptionalVector<int64_t>& data);
ParquetColumn MakeFloatColumn(const std::string& name, int field_id, const OptionalVector<float>& data);
ParquetColumn MakeDoubleColumn(const std::string& name, int field_id, const OptionalVector<double>& data);
ParquetColumn MakeStringColumn(const std::string& name, int field_id, const std::vector<std::string*>& data);
ParquetColumn MakeBinaryColumn(const std::string& name, int field_id, const std::vector<std::string*>& data);
ParquetColumn MakeJsonColumn(const std::string& name, int field_id, const std::vector<std::string*>& data);
ParquetColumn MakeUuidColumn(const std::string& name, int field_id, const std::vector<std::string*>& data);
ParquetColumn MakeTimeColumn(const std::string& name, int field_id, const OptionalVector<int64_t>& data);
ParquetColumn MakeTimestampColumn(const std::string& name, int field_id, const OptionalVector<int64_t>& data);
ParquetColumn MakeTimestamptzColumn(const std::string& name, int field_id, const OptionalVector<int64_t>& data);
ParquetColumn MakeTimestampNsColumn(const std::string& name, int field_id, const OptionalVector<int64_t>& data);
ParquetColumn MakeTimestamptzNsColumn(const std::string& name, int field_id, const OptionalVector<int64_t>& data);
ParquetColumn MakeDateColumn(const std::string& name, int field_id, const OptionalVector<int32_t>& data);
ParquetColumn MakeNumericColumn(const std::string& name, int field_id, const OptionalVector<int32_t>& data,
                                int precision, int scale);
ParquetColumn MakeNumericColumn(const std::string& name, int field_id, const OptionalVector<int64_t>& data,
                                int precision, int scale);
ParquetColumn MakeNumericColumn(const std::string& name, int field_id, const std::vector<std::string*>& data,
                                int precision, int scale, int length = -1);

}  // namespace iceberg
