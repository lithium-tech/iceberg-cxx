#pragma once

#include <algorithm>

#include <arrow/status.h>

#include <cstdint>

#include <iceberg/common/rg_metadata.h>
#include <parquet/metadata.h>

#include "iceberg/common/batch.h"
#include "iceberg/common/fs/file_reader_provider.h"
#include "iceberg/common/rg_metadata.h"
#include "iceberg/streams/iceberg/data_entries_meta_stream.h"
#include "iceberg/streams/iceberg/data_scan.h"
#include "iceberg/streams/iceberg/equality_delete_applier.h"
#include "iceberg/streams/iceberg/filtering_stream.h"
#include "iceberg/streams/iceberg/mapper.h"
#include "iceberg/streams/iceberg/plan.h"
#include "iceberg/streams/iceberg/row_group_filter.h"
#include "parquet/type_fwd.h"

namespace iceberg {

// TODO(gmusya): maybe extract some private methods into functions
class FileReaderBuilder : public DataScanner::IIcebergStreamBuilder {
 public:
  FileReaderBuilder(std::vector<int> field_ids_to_retrieve, std::shared_ptr<const EqualityDeletes> equality_deletes,
                    std::shared_ptr<const FieldIdMapper> mapper,
                    std::shared_ptr<const IFileReaderProvider> file_reader_provider,
                    std::shared_ptr<const IRowGroupFilter> rg_filter,
                    std::shared_ptr<const ice_filter::IRowFilter> row_filter, std::shared_ptr<ILogger> logger = nullptr)
      : field_ids_to_retrieve_(std::move(field_ids_to_retrieve)),
        equality_deletes_(equality_deletes),
        mapper_(mapper),
        file_reader_provider_(file_reader_provider),
        rg_filter_(rg_filter),
        row_filter_(row_filter),
        logger_(logger) {
    Ensure(equality_deletes_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": equality_deletes is nullptr");
    Ensure(mapper_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": mapper is nullptr");
    Ensure(file_reader_provider_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": file_reader_provider is nullptr");
  }

  IcebergStreamPtr Build(const AnnotatedDataPath& annotated_data_path) override;

 private:
  std::shared_ptr<parquet::arrow::FileReader> MakeArrowReader(const std::string& path) {
    auto input_file = iceberg::ValueSafe(file_reader_provider_->Open(path));
    return input_file;
  }

  struct ParquetColumnInfo {
    int field_id;

    std::optional<int> column_position;
    std::string parquet_name;

    std::string result_name;

    ParquetColumnInfo() = delete;
    ParquetColumnInfo(int f, std::optional<int> pos, std::string par_name, std::string res_name)
        : field_id(f), column_position(pos), parquet_name(std::move(par_name)), result_name(std::move(res_name)) {}
  };

  std::vector<ParquetColumnInfo> GetColumnPositionsToRetrieveByFieldIds(const parquet::FileMetaData& metadata,
                                                                        const std::vector<int>& field_ids) const {
    const auto& schema = metadata.schema();
    Ensure(schema != nullptr, std::string(__PRETTY_FUNCTION__) + ": schema is nullptr");

    const auto& group_node = schema->group_node();
    Ensure(group_node != nullptr, std::string(__PRETTY_FUNCTION__) + ": group_node is nullptr");

    const auto field_count = group_node->field_count();

    std::vector<ParquetColumnInfo> column_infos;

    for (const auto& field_id_to_find : field_ids) {
      bool found = false;

      for (int i = 0; i < field_count; ++i) {
        auto field = group_node->field(i);
        Ensure(field != nullptr, std::string(__PRETTY_FUNCTION__) + ": field is nullptr");

        const int current_field_id = field->field_id();
        if (field_id_to_find == current_field_id) {
          found = true;
          auto result_name = mapper_->FieldIdToColumnName(field_id_to_find);
          column_infos.emplace_back(field_id_to_find, i, field->name(), std::move(result_name));
          break;
        }
      }

      if (!found) {
        auto result_name = mapper_->FieldIdToColumnName(field_id_to_find);
        column_infos.emplace_back(field_id_to_find, std::nullopt, "", std::move(result_name));
      }
    }

    return column_infos;
  }

  static std::vector<int> GetRowGroupsToRetrieve(const parquet::FileMetaData& metadata,
                                                 const std::vector<AnnotatedDataPath::Segment>& segments) {
    std::vector<int> row_groups_to_process;

    size_t last_not_mapped_part_id = 0;
    for (int i = 0; i < metadata.num_row_groups(); ++i) {
      auto rg_meta = metadata.RowGroup(i);
      Ensure(rg_meta != nullptr, std::string(__PRETTY_FUNCTION__) + ": rg_meta is nullptr");

      int64_t file_offset = RowGroupMetaToFileOffset(*rg_meta);
      while (last_not_mapped_part_id < segments.size()) {
        const auto seg_length = segments[last_not_mapped_part_id].length;  // 0 <=> to end
        const auto& seg_start = segments[last_not_mapped_part_id].offset;
        const auto& seg_end = segments[last_not_mapped_part_id].offset + segments[last_not_mapped_part_id].length;

        if (seg_start <= file_offset && (file_offset < seg_end || seg_length == 0)) {
          row_groups_to_process.emplace_back(i);
          break;
        }
        if (file_offset >= seg_end) {
          ++last_not_mapped_part_id;
          continue;
        }
        break;
      }
    }
    return row_groups_to_process;
  }

  StreamPtr<ArrowBatchWithRowPosition> MakeFinalStream(std::shared_ptr<parquet::arrow::FileReader> arrow_reader,
                                                       const std::vector<int>& matching_row_groups,
                                                       std::shared_ptr<const parquet::FileMetaData> metadata,
                                                       const std::vector<int>& field_ids);

  const std::vector<ColumnInfo> columns_to_retrieve_;
  const std::vector<int> field_ids_to_retrieve_;
  std::shared_ptr<const EqualityDeletes> equality_deletes_;
  std::shared_ptr<const FieldIdMapper> mapper_;
  std::shared_ptr<const IFileReaderProvider> file_reader_provider_;
  std::shared_ptr<const IRowGroupFilter> rg_filter_;
  std::shared_ptr<const ice_filter::IRowFilter> row_filter_;
  std::shared_ptr<ILogger> logger_;
};

}  // namespace iceberg
