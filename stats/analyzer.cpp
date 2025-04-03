#include "stats/analyzer.h"

#include <iostream>
#include <stdexcept>
#include <string>

#include "arrow/status.h"
#include "parquet/column_reader.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/schema.h"
#include "parquet/type_fwd.h"
#include "parquet/types.h"
#include "stats/data.h"
#include "stats/parquet/distinct.h"
#include "stats/parquet/frequent_item.h"
#include "stats/parquet/quantiles.h"
#include "stats/types.h"

namespace stats {
namespace {

template <ParquetType parquet_type>
GenericParquetReader MakeGenericParquetReader(const GenericParquetBuffer& buffer,
                                              std::shared_ptr<parquet::ColumnReader> col_reader, int batch_size) {
  auto typed_buffer = buffer.Get<parquet_type>();
  auto typed_reader =
      std::static_pointer_cast<typename parquet::TypedColumnReader<parquet::PhysicalType<parquet_type>>>(col_reader);
  auto result_reader = std::make_shared<ParquetReader<parquet_type>>(typed_reader, typed_buffer, batch_size);
  return GenericParquetReader(result_reader);
}

std::optional<GenericParquetReader> MakeGenericParquetReaderFromType(const GenericParquetBuffer& buffer,
                                                                     std::shared_ptr<parquet::ColumnReader> col_reader,
                                                                     int batch_size, ParquetType parquet_type) {
  switch (parquet_type) {
    case parquet::Type::INT32:
      return MakeGenericParquetReader<ParquetType::INT32>(buffer, col_reader, batch_size);
    case parquet::Type::INT64:
      return MakeGenericParquetReader<ParquetType::INT64>(buffer, col_reader, batch_size);
    case parquet::Type::BYTE_ARRAY:
      return MakeGenericParquetReader<ParquetType::BYTE_ARRAY>(buffer, col_reader, batch_size);
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      return MakeGenericParquetReader<ParquetType::FIXED_LEN_BYTE_ARRAY>(buffer, col_reader, batch_size);
    // not supported currently
    case parquet::Type::INT96:
    case parquet::Type::FLOAT:
    case parquet::Type::DOUBLE:
    case parquet::Type::BOOLEAN:
    case parquet::Type::UNDEFINED:
      return std::nullopt;
  }
  throw arrow::Status::ExecutionError(__PRETTY_FUNCTION__, ": internal error");
}

std::optional<GenericParquetBuffer> MakeBufferFromType(int32_t batch_size, ParquetType type) {
  switch (type) {
    case parquet::Type::INT32: {
      auto buffer = std::make_shared<Int32ParquetBuffer>(batch_size);
      return GenericParquetBuffer(buffer);
    }
    case parquet::Type::INT64: {
      auto buffer = std::make_shared<Int64ParquetBuffer>(batch_size);
      return GenericParquetBuffer(buffer);
    }
    case parquet::Type::BYTE_ARRAY: {
      auto buffer = std::make_shared<ByteArrayParquetBuffer>(batch_size);
      return GenericParquetBuffer(buffer);
    }
    case parquet::Type::FIXED_LEN_BYTE_ARRAY: {
      auto buffer = std::make_shared<FLBAParquetBuffer>(batch_size);
      return GenericParquetBuffer(buffer);
    }
    // not supported currently
    case parquet::Type::INT96:
    case parquet::Type::FLOAT:
    case parquet::Type::DOUBLE:
    case parquet::Type::BOOLEAN:
    case parquet::Type::UNDEFINED:
      return std::nullopt;
  }
  throw arrow::Status::ExecutionError(__PRETTY_FUNCTION__, ": internal error");
}

CommonDistinctWrapper MakeDistinctCounter(DistinctCounterImplType type) {
  switch (type) {
    case DistinctCounterImplType::kNaive:
      return CommonDistinctWrapper(stats::NaiveDistinctCounterWrapper());
    case DistinctCounterImplType::kTheta:
      return CommonDistinctWrapper(stats::ThetaDistinctCounterWrapper());
    case DistinctCounterImplType::kHyperLogLog:
      return CommonDistinctWrapper(stats::HLLDistinctCounterWrapper());
  }
  throw arrow::Status::ExecutionError(__PRETTY_FUNCTION__, ": internal error");
}

CommonQuantileWrapper MakeQuantileCounter(parquet::Type::type type) {
  const bool is_string_type = type == parquet::Type::FIXED_LEN_BYTE_ARRAY || type == parquet::Type::BYTE_ARRAY;
  const auto stats_type = is_string_type ? stats::Type::kString : stats::Type::kInt64;

  return CommonQuantileWrapper(stats_type);
}

CommonFrequentItemsWrapper MakeFrequentItemsCounter(parquet::Type::type type) {
  const bool is_string_type = type == parquet::Type::FIXED_LEN_BYTE_ARRAY || type == parquet::Type::BYTE_ARRAY;
  const auto stats_type = is_string_type ? stats::Type::kString : stats::Type::kInt64;

  return CommonFrequentItemsWrapper(stats_type);
}
}  // namespace

AnalyzeColumnResult Analyzer::InitializeSketchesForColumn(const parquet::ColumnDescriptor& descriptor) {
  AnalyzeColumnResult column_result;
  auto type = descriptor.physical_type();

  column_result.type = type;
  column_result.field_id = descriptor.schema_node()->field_id();
  column_result.counter = MakeDistinctCounter(settings_.distinct_counter_implementation);
  if (settings_.evaluate_quantiles) {
    column_result.quantile_sketch.emplace(MakeQuantileCounter(type));
  }
  if (settings_.evaluate_frequent_items) {
    column_result.frequent_items_sketch.emplace(MakeFrequentItemsCounter(type));
  }

  if (type == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
    auto len = descriptor.type_length();
    if (column_result.counter) {
      column_result.counter->SetFLBALength(len);
    }
    if (column_result.quantile_sketch) {
      column_result.quantile_sketch->SetFLBALength(len);
    }
    if (column_result.frequent_items_sketch) {
      column_result.frequent_items_sketch->SetFLBALength(len);
    }
  }
  return column_result;
}

static bool IsDictionaryEncoding(parquet::Encoding::type type) {
  switch (type) {
    case parquet::Encoding::PLAIN_DICTIONARY:
    case parquet::Encoding::RLE_DICTIONARY:
      return true;
    case parquet::Encoding::PLAIN:
    case parquet::Encoding::RLE:
    case parquet::Encoding::BIT_PACKED:
    case parquet::Encoding::DELTA_BINARY_PACKED:
    case parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY:
    case parquet::Encoding::DELTA_BYTE_ARRAY:
    case parquet::Encoding::BYTE_STREAM_SPLIT:
    case parquet::Encoding::UNDEFINED:
    case parquet::Encoding::UNKNOWN:
      return false;
  }
  throw arrow::Status::ExecutionError(__PRETTY_FUNCTION__, ": internal error");
}

static bool AllDataPagesAreEncodedWithDictionary(const parquet::ColumnChunkMetaData& column_metadata) {
  auto encoding_stats = column_metadata.encoding_stats();

  if (!column_metadata.has_dictionary_page()) {
    return false;
  }

  // check that all data pages are encoded with dictionary encoding
  for (auto stat : encoding_stats) {
    switch (stat.page_type) {
      case parquet::PageType::DICTIONARY_PAGE:
      case parquet::PageType::INDEX_PAGE:
      case parquet::PageType::UNDEFINED:
        return false;
      case parquet::PageType::DATA_PAGE:
      case parquet::PageType::DATA_PAGE_V2:
        if (!IsDictionaryEncoding(stat.encoding)) {
          return false;
        }
    }
  }

  return true;
}

bool Analyzer::EvaluateStatsFromDictionaryPage(const parquet::RowGroupMetaData& rg_metadata, int col,
                                               parquet::RowGroupReader& rg_reader) {
  auto col_metadata = rg_metadata.ColumnChunk(col);
  if (!col_metadata) {
    throw arrow::Status::ExecutionError("No column metadata for column ", col);
  }

  if (!AllDataPagesAreEncodedWithDictionary(*col_metadata)) {
    return false;
  }

  auto col_name = rg_metadata.schema()->Column(col)->name();

  auto& column_result = result_.sketches.at(col_name);
  auto& counter_for_col = *column_result.counter;

  auto record_reader = rg_reader.RecordReader(col, true);
  if (!record_reader) {
    throw arrow::Status::ExecutionError("No record reader for col ", col);
  }
  int32_t dict_len;
  auto* dict_data = record_reader->ReadDictionary(&dict_len);
  auto type = col_metadata->type();
  counter_for_col.AppendValues(dict_data, dict_len, type);
  return true;
}

void Analyzer::AnalyzeColumn(const parquet::RowGroupMetaData& rg_metadata, int col,
                             parquet::RowGroupReader& rg_reader) {
  auto col_metadata = rg_metadata.ColumnChunk(col);
  if (!col_metadata) {
    throw arrow::Status::ExecutionError("No column metadata for col ", col);
  }
  auto col_name = rg_metadata.schema()->Column(col)->name();
  auto type = col_metadata->type();

  auto& column_result = result_.sketches.at(col_name);

  auto& counter_for_col = *column_result.counter;
  bool distinct_is_evaluated = false;

  if (settings_.use_dictionary_optimization) {
    distinct_is_evaluated = EvaluateStatsFromDictionaryPage(rg_metadata, col, rg_reader);
  }

  if (distinct_is_evaluated && !settings_.evaluate_frequent_items && !settings_.evaluate_quantiles) {
    return;
  }

  int64_t total_values = col_metadata->num_values();
  int64_t values_read = 0;
  auto col_reader = rg_reader.Column(col);

  const auto& buffer = buffers.at(col_name);

  std::optional<GenericParquetReader> reader =
      MakeGenericParquetReaderFromType(buffer, col_reader, settings_.batch_size, type);
  if (!reader.has_value()) {
    return;
  }

  while (values_read < total_values) {
    auto generic_buffer = reader->ReadGeneric();
    if (!distinct_is_evaluated) {
      counter_for_col.AppendValues(generic_buffer.data, generic_buffer.values_size, type);
    }
    if (settings_.evaluate_quantiles) {
      auto& quantiles_for_col = *column_result.quantile_sketch;
      quantiles_for_col.AppendValues(generic_buffer.data, generic_buffer.values_size, type);
    }

    if (settings_.evaluate_frequent_items) {
      auto& frequent_items_for_col = *column_result.frequent_items_sketch;
      frequent_items_for_col.AppendValues(generic_buffer.data, generic_buffer.values_size, type);
    }
    values_read += generic_buffer.values_size;
  }
}

void Analyzer::Analyze(const std::string& filename) {
  auto maybe_input_file = settings_.fs->OpenInputFile(filename);
  if (!maybe_input_file.ok()) {
    throw maybe_input_file.status();
  }
  auto input_file = maybe_input_file.MoveValueUnsafe();

  auto file_reader = parquet::ParquetFileReader::Open(input_file);
  auto metadata = file_reader->metadata();
  auto num_row_groups = metadata->num_row_groups();
  auto num_columns = metadata->num_columns();
  auto num_rows = metadata->num_rows();

  for (int col = 0; col < num_columns; ++col) {
    const auto* column_descriptor = metadata->schema()->Column(col);
    if (!column_descriptor) {
      throw arrow::Status::ExecutionError("No column descriptor for column ", col);
    }
    const auto& name = column_descriptor->name();

    if (settings_.columns_to_ignore.contains(name)) {
      continue;
    }

    const auto type = column_descriptor->physical_type();
    if (result_.sketches.contains(name)) {
      if (result_.sketches.at(name).type != type) {
        throw arrow::Status::ExecutionError("File ", filename, " contains unexpected type ", type, " for column '",
                                            name, "'");
      }
      continue;
    }

    auto maybe_buffer = MakeBufferFromType(settings_.batch_size, type);
    if (maybe_buffer.has_value()) {
      buffers.emplace(name, std::move(maybe_buffer.value()));
    } else {
      throw arrow::Status::ExecutionError("Type ", static_cast<int>(type), " is not supported for column '", name, "'");
    }

    result_.sketches.emplace(name, InitializeSketchesForColumn(*column_descriptor));
  }

  std::cerr << filename << ": num_row_groups = " << num_row_groups << std::endl;
  std::cerr << filename << ": num_columns = " << num_columns << std::endl;
  std::cerr << filename << ": num_rows = " << num_rows << std::endl;

  for (auto rg = 0; rg < num_row_groups; ++rg) {
    auto rg_reader = file_reader->RowGroup(rg);
    auto rg_metadata = rg_reader->metadata();
    if (!rg_metadata) {
      throw arrow::Status::ExecutionError("No RowGroupMetadata for row group ", rg);
    }
    if (!rg_reader) {
      throw arrow::Status::ExecutionError("No RowGroupReader for row group ", rg);
    }
    for (auto col = 0; col < num_columns; ++col) {
      const auto* column_descriptor = metadata->schema()->Column(col);
      const auto& name = column_descriptor->name();
      if (settings_.columns_to_ignore.contains(name)) {
        continue;
      }
      AnalyzeColumn(*rg_metadata, col, *rg_reader.get());
    }
  }
}

}  // namespace stats
