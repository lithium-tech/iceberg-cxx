#pragma once

#include <arrow/status.h>

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/common/logger.h"
#include "iceberg/equality_delete/handler.h"
#include "iceberg/schema.h"
#include "iceberg/streams/iceberg/data_entries_meta_stream.h"
#include "iceberg/streams/iceberg/data_scan.h"
#include "iceberg/streams/iceberg/equality_delete_applier.h"
#include "iceberg/streams/iceberg/file_reader_builder.h"
#include "iceberg/streams/iceberg/iceberg_batch.h"
#include "iceberg/streams/iceberg/mapper.h"
#include "iceberg/streams/iceberg/positional_delete_applier.h"
#include "iceberg/streams/iceberg/projection_stream.h"
#include "iceberg/streams/iceberg/row_group_filter.h"

namespace iceberg {

class IcebergScanBuilder {
 public:
  static IcebergStreamPtr MakeIcebergStream(
      AnnotatedDataPathStreamPtr meta_stream, PositionalDeletes positional_deletes,
      std::shared_ptr<EqualityDeletes> equality_deletes, std::optional<EqualityDeleteHandler::Config> cfg,
      std::shared_ptr<const IRowGroupFilter> rg_filter, std::shared_ptr<const ice_filter::IRowFilter> row_filter,
      const iceberg::Schema& schema, std::vector<int> field_ids_to_retrieve,
      std::shared_ptr<const IFileReaderProvider> file_reader_provider,
      std::optional<std::string> schema_name_mapping = std::nullopt, std::shared_ptr<ILogger> logger = nullptr) {
    Ensure(file_reader_provider != nullptr, std::string(__PRETTY_FUNCTION__) + ": file_reader_provider is nullptr");

    auto mapper = MakeMapper(schema);

<<<<<<< HEAD
    std::optional<SchemaNameMapper> schema_name_mapper;
    if (schema_name_mapping.has_value()) {
      schema_name_mapper = SchemaNameMapper(*schema_name_mapping);
    }

    // this class takes an AnnotatedDataPath as input and returns IcebergStream
    // (which reads some columns from row groups matching rg-filter of data file)
    auto stream_builder =
        std::make_shared<FileReaderBuilder>(field_ids_to_retrieve, equality_deletes, mapper, file_reader_provider,
                                            rg_filter, row_filter, std::move(schema_name_mapper), logger);
=======
    auto default_value_map = MakeDefaultValueMap(schema);

    // this class takes an AnnotatedDataPath as input and returns IcebergStream
    // (which reads some columns from row groups matching rg-filter of data file)
    auto stream_builder = std::make_shared<FileReaderBuilder>(
        field_ids_to_retrieve, equality_deletes, mapper, file_reader_provider, rg_filter,
        std::move(schema_name_mapping), MakeDefaultValueMap(schema), logger);
>>>>>>> 661b90c (tmp)

    // convert stream of AnnotatedDatapath into concatenation of streams created with FileReaderBuilder
    IcebergStreamPtr stream = std::make_shared<DataScanner>(meta_stream, stream_builder);

    if (!equality_deletes->partlayer_to_deletes.empty()) {
      Ensure(cfg.has_value(), std::string(__PRETTY_FUNCTION__) +
                                  ": equality deletes are present, but config for equality deletes is not set");
      stream = std::make_shared<EqualityDeleteApplier>(stream, equality_deletes, cfg.value(), mapper,
                                                       file_reader_provider, logger);
    }

    if (!positional_deletes.delete_entries.empty()) {
      stream = std::make_shared<PositionalDeleteApplier>(stream, std::move(positional_deletes), file_reader_provider,
                                                         logger);
    }

    stream = MakeFinalProjection(*mapper, stream, field_ids_to_retrieve);

    return stream;
  }

 private:
  static std::shared_ptr<FieldIdMapper> MakeMapper(const iceberg::Schema& schema) {
    std::map<int, std::string> field_id_to_column_name;
    for (const auto& col : schema.Columns()) {
      field_id_to_column_name[col.field_id] = col.name;
    }

    // all mappings must use field ids, not names
    // arrow::RecordBatch store names, not field ids
    // this class helps with mapping from parquet field id to arrow::RecordBatch names
    return std::make_shared<FieldIdMapper>(std::move(field_id_to_column_name));
  }

  static std::shared_ptr<const std::map<int, Literal>> MakeDefaultValueMap(const iceberg::Schema& schema) {
    std::map<int, Literal> field_id_to_default_value;
    for (const auto& col : schema.Columns()) {
      if (col.initial_default.has_value()) {
        field_id_to_default_value.insert({col.field_id, col.initial_default.value()});
      }
    }

    // all mappings must use field ids, not names
    // arrow::RecordBatch store names, not field ids
    // this class helps with mapping from parquet field id to arrow::RecordBatch names
    return std::make_shared<const std::map<int, Literal>>(std::move(field_id_to_default_value));
  }

  static std::shared_ptr<IcebergStream> MakeFinalProjection(const FieldIdMapper& mapper,
                                                            std::shared_ptr<IcebergStream> stream,
                                                            const std::vector<int>& field_ids_to_retrieve) {
    std::map<std::string, std::string> projection;
    for (int field_id : field_ids_to_retrieve) {
      std::string name = mapper.FieldIdToColumnName(field_id);
      projection[name] = name;
    }

    return std::make_shared<IcebergProjectionStream>(std::move(projection), stream);
  }
};

}  // namespace iceberg
