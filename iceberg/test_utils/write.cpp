#include "iceberg/test_utils/write.h"

#include <arrow/status.h>

#include <memory>
#include <string>
#include <vector>

#include "iceberg/test_utils/column.h"
#include "parquet/column_writer.h"
#include "parquet/file_writer.h"
#include "parquet/types.h"

namespace iceberg {
namespace {

using parquet::ParquetFileWriter;
using parquet::Repetition;
using parquet::schema::GroupNode;
using WriteProc = void (*)(parquet::ColumnWriter* writer, const ParquetColumnData& data, size_t position);

template <typename TWriter>
void WriteNextPrimitive(parquet::ColumnWriter* writer, const ParquetColumnData& data, size_t position) {
  auto r = static_cast<TWriter*>(writer);
  const auto& typed_data = std::get<OptionalVector<typename TWriter::T>>(data);
  const auto& value = typed_data[position];
  if (value.has_value()) {
    int16_t def = 1;
    r->WriteBatch(1, &def, nullptr, &(*value));
  } else {
    int16_t def = 0;
    r->WriteBatch(1, &def, nullptr, nullptr);
  }
}

WriteProc GetWriteProc(const parquet::Type::type type) noexcept {
  switch (type) {
    case parquet::Type::BOOLEAN:
      return &WriteNextPrimitive<parquet::BoolWriter>;
    case parquet::Type::INT32:
      return &WriteNextPrimitive<parquet::Int32Writer>;
    case parquet::Type::INT64:
      return &WriteNextPrimitive<parquet::Int64Writer>;
    case parquet::Type::FLOAT:
      return &WriteNextPrimitive<parquet::FloatWriter>;
    case parquet::Type::DOUBLE:
      return &WriteNextPrimitive<parquet::DoubleWriter>;
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      return &WriteNextPrimitive<parquet::FixedLenByteArrayWriter>;
    case parquet::Type::BYTE_ARRAY:
      return &WriteNextPrimitive<parquet::ByteArrayWriter>;
    case parquet::Type::INT96:
    case parquet::Type::UNDEFINED:
      return nullptr;
  }
  return nullptr;
}

template <typename TWriter>
void WriteNextArray(parquet::ColumnWriter* writer, const ParquetColumnData& data, size_t position) {
  // Write arrays with three-level list representation.
  // https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#lists
  //
  // optional group <name> (LIST) {
  //   repeated group list {
  //     optional <element-type> element;
  //   }
  // }
  const auto& arr = std::get<ArrayContainer>(data).arrays[position];
  const auto& typed_data = std::get<OptionalVector<typename TWriter::T>>(arr);
  auto typed_writer = static_cast<TWriter*>(writer);
  int16_t rep = writer->descr()->max_repetition_level() - 1;
  for (const auto& value : typed_data) {
    if (value.has_value()) {
      int16_t def = writer->descr()->max_definition_level();
      typed_writer->WriteBatch(1, &def, &rep, &(*value));
    } else {
      int16_t def = writer->descr()->max_definition_level() - 1;
      typed_writer->WriteBatch(1, &def, &rep, nullptr);
    }
    rep = writer->descr()->max_repetition_level();
  }
}

WriteProc GetWriteProcRepeated(const parquet::Type::type type) noexcept {
  switch (type) {
    case parquet::Type::BOOLEAN:
      return &WriteNextArray<parquet::BoolWriter>;
    case parquet::Type::INT32:
      return &WriteNextArray<parquet::Int32Writer>;
    case parquet::Type::INT64:
      return &WriteNextArray<parquet::Int64Writer>;
    case parquet::Type::FLOAT:
      return &WriteNextArray<parquet::FloatWriter>;
    case parquet::Type::DOUBLE:
      return &WriteNextArray<parquet::DoubleWriter>;
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      return &WriteNextArray<parquet::FixedLenByteArrayWriter>;
    case parquet::Type::BYTE_ARRAY:
      return &WriteNextArray<parquet::ByteArrayWriter>;
    case parquet::Type::INT96:
    case parquet::Type::UNDEFINED:
      return nullptr;
  }
  return nullptr;
}

void WriteToFile(const Table& table, ParquetFileWriter* writer) {
  size_t row = 0;
  for (size_t row_group = 0; row_group < table.row_group_sizes.size(); ++row_group) {
    auto rg_writer = writer->AppendRowGroup();
    for (const auto& column : table.columns) {
      WriteProc write_proc = column.info.repetition == parquet::Repetition::REPEATED
                                 ? GetWriteProcRepeated(column.info.physical_type)
                                 : GetWriteProc(column.info.physical_type);
      parquet::ColumnWriter* row_writer = rg_writer->NextColumn();
      for (size_t row_in_group = 0; row_in_group < table.row_group_sizes[row_group]; ++row_in_group) {
        write_proc(row_writer, column.data, row + row_in_group);
      }
    }
    row += table.row_group_sizes[row_group];
  }
}

std::shared_ptr<parquet::schema::GroupNode> GetSchema(const std::vector<ParquetColumn>& columns) {
  parquet::schema::NodeVector fields;
  fields.reserve(columns.size());
  for (const auto& column : columns) {
    fields.push_back(column.info.MakeField());
  }
  return std::static_pointer_cast<GroupNode>(GroupNode::Make("schema", Repetition::REQUIRED, fields));
}
}  // namespace

arrow::Status WriteToFile(const Table& table, const std::string& file_path) {
  std::string path;
  auto fs = arrow::fs::FileSystemFromUri(file_path, &path).ValueOrDie();

  auto schema = GetSchema(table.columns);
  ARROW_ASSIGN_OR_RAISE(auto out_stream, fs->OpenOutputStream(path));

  auto file_writer = parquet::ParquetFileWriter::Open(out_stream, schema);
  WriteToFile(table, file_writer.get());
  file_writer->Close();
  ARROW_RETURN_NOT_OK(out_stream->Close());
  return arrow::Status::OK();
}

arrow::Status WriteToFile(const std::vector<ParquetColumn>& columns, const std::string& file_path) {
  if (columns.empty()) {
    return arrow::Status::ExecutionError("Cannot write empty table");
  }
  for (size_t i = 0; i < columns.size(); ++i) {
    if (columns[i].Size() != columns[0].Size()) {
      return arrow::Status::ExecutionError("Test error: column 0 has size ", columns[0].Size(), " but column ", i,
                                           " has size ", columns[i].Size());
    }
  }
  return WriteToFile({columns, {columns[0].Size()}}, file_path);
}

arrow::Status WriteToFile(const ParquetColumn& column, const std::string& file_path) {
  return WriteToFile(std::vector<ParquetColumn>{column}, file_path);
}

}  // namespace iceberg
