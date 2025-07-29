#include <fstream>

#include "gtest/gtest.h"
#include "iceberg/common/fs/file_reader_provider_impl.h"
#include "iceberg/common/fs/filesystem_provider_impl.h"
#include "iceberg/streams/iceberg/builder.h"
#include "iceberg/table_metadata.h"

namespace iceberg {
namespace {

class MetaStream : public IAnnotatedDataPathStream {
 public:
  explicit MetaStream(ice_tea::ScanMetadata&& planned_meta) {
    planned_meta_ = std::move(planned_meta);

    InitializeAllEntries();
  }

  std::shared_ptr<AnnotatedDataPath> ReadNext() override {
    if (current_iterator_ == all_data_entries.size()) {
      return nullptr;
    }
    return std::make_shared<AnnotatedDataPath>(all_data_entries[current_iterator_++]);
  }

 private:
  void InitializeAllEntries() {
    for (size_t partition_id = 0; partition_id < planned_meta_.partitions.size(); ++partition_id) {
      for (size_t layer_id_p1 = planned_meta_.partitions[partition_id].size(); layer_id_p1 >= 1; layer_id_p1--) {
        const auto layer_id = layer_id_p1 - 1;

        std::sort(planned_meta_.partitions[partition_id][layer_id].data_entries_.begin(),
                  planned_meta_.partitions[partition_id][layer_id].data_entries_.end(),
                  [&](const auto& lhs, const auto& rhs) { return lhs.path < rhs.path; });
        for (const auto& data_entry : planned_meta_.partitions[partition_id][layer_id].data_entries_) {
          std::vector<AnnotatedDataPath::Segment> segments;
          for (const auto& part : data_entry.parts) {
            segments.emplace_back(AnnotatedDataPath::Segment{.offset = part.offset, .length = part.length});
          }
          PartitionLayerFile state(PartitionLayer(partition_id, layer_id), data_entry.path);

          auto path = AnnotatedDataPath(state, std::move(segments));
          all_data_entries.emplace_back(std::move(path));
        }
      }
    }
  }

  std::vector<AnnotatedDataPath> all_data_entries;
  ice_tea::ScanMetadata planned_meta_;
  size_t current_iterator_ = 0;
};

void Ensure(bool cond, const std::string& msg) {
  if (!cond) {
    throw std::runtime_error(msg);
  }
}

IcebergStreamPtr MakeDataStream(const std::string& path, const std::vector<int>& field_ids_to_retrieve) {
  std::ifstream input(path);

  auto metadata = ice_tea::ReadTableMetadataV2(input);
  Ensure(!!metadata, "Failed to read metadata");

  auto fs = std::make_shared<arrow::fs::LocalFileSystem>();

  auto entries_stream = ice_tea::AllEntriesStream::Make(fs, metadata);
  Ensure(!!entries_stream, "Failed to make AllEntriesStream");

  auto maybe_scan_metadata = ice_tea::GetScanMetadata(*entries_stream, *metadata);
  Ensure(maybe_scan_metadata.ok(), "Failed to get scan metadata");

  auto scan_metadata = maybe_scan_metadata.ValueUnsafe();
  auto meta_stream = std::make_shared<MetaStream>(std::move(scan_metadata));

  std::shared_ptr<IFileSystemGetter> local_fs_getter = std::make_shared<LocalFileSystemGetter>();

  std::map<std::string, std::shared_ptr<IFileSystemGetter>> schema_to_getter{{"s3a", local_fs_getter}};

  std::shared_ptr<IFileSystemProvider> fs_provider = std::make_shared<FileSystemProvider>(schema_to_getter);

  PositionalDeletes pos_del_info;
  EqualityDeletes eq_del_info;

  for (size_t partition_id = 0; partition_id < scan_metadata.partitions.size(); ++partition_id) {
    const auto& partition = scan_metadata.partitions.at(partition_id);
    for (size_t layer_id = 0; layer_id < partition.size(); ++layer_id) {
      const auto& layer = partition[layer_id];
      pos_del_info.delete_entries[partition_id][layer_id] = std::move(layer.positional_delete_entries_);
      eq_del_info.partlayer_to_deletes[partition_id][layer_id] = std::move(layer.equality_delete_entries_);
    }
  }

  EqualityDeleteHandler::Config eq_del_config;
  eq_del_config.use_specialized_deletes = false;
  eq_del_config.equality_delete_max_mb_size = 10;
  eq_del_config.max_rows = 1'000'000;
  eq_del_config.throw_if_memory_limit_exceeded = false;

  auto schema_name_mapping = [metadata]() -> std::optional<std::string> {
    auto it = metadata->properties.find("schema.name-mapping.default");
    if (it == metadata->properties.end()) {
      return std::nullopt;
    }
    return it->second;
  }();

  return IcebergScanBuilder::MakeIcebergStream(
      meta_stream, pos_del_info, std::make_shared<EqualityDeletes>(std::move(eq_del_info)), std::move(eq_del_config),
      nullptr, *metadata->GetCurrentSchema(), field_ids_to_retrieve, std::make_shared<FileReaderProvider>(fs_provider),
      schema_name_mapping);
}

TEST(StreamsTest, EndToEndNullColumn) {
  const std::string path =
      "warehouse/streams/default_value/metadata/00014-c179c339-e10a-486b-b075-144f5e5b101a.metadata.json";
  std::vector<int> field_ids_to_retrieve(13);
  std::iota(field_ids_to_retrieve.begin(), field_ids_to_retrieve.end(), 1);
  auto data_stream = MakeDataStream(path, field_ids_to_retrieve);

  /*auto batch1 = data_stream->ReadNext();
  ASSERT_TRUE(!!batch1);

  auto batch2 = data_stream->ReadNext();
  ASSERT_TRUE(!!batch2);*/

  while (true) {
    auto batch = data_stream->ReadNext();
    if (!batch) {
      break;
    }
    std::cout << "path: " << batch->GetPath() << ", row_number = " << batch->GetRowPosition()
              << ", partition_id = " << batch->GetPartition() << ", layer_id = " << batch->GetLayer() << std::endl;
    std::cout << "batch: " << batch->GetRecordBatch()->ToString() << std::endl;
    std::cout << std::string(80, '-') << std::endl;
  }
}

TEST(StreamsTest, EndToEndWithDefaultValue) {
  const std::string path = "warehouse/streams/default_value/metadata/DefaultValues.json";
  std::vector<int> field_ids_to_retrieve(13);
  std::iota(field_ids_to_retrieve.begin(), field_ids_to_retrieve.end(), 1);
  auto data_stream = MakeDataStream(path, field_ids_to_retrieve);

  /*auto batch1 = data_stream->ReadNext();
  ASSERT_TRUE(!!batch1);

  auto batch2 = data_stream->ReadNext();
  ASSERT_TRUE(!!batch2);*/

  while (true) {
    auto batch = data_stream->ReadNext();
    if (!batch) {
      break;
    }
    std::cout << "path: " << batch->GetPath() << ", row_number = " << batch->GetRowPosition()
              << ", partition_id = " << batch->GetPartition() << ", layer_id = " << batch->GetLayer() << std::endl;
    std::cout << "batch: " << batch->GetRecordBatch()->ToString() << std::endl;
    std::cout << std::string(80, '-') << std::endl;
  }
}

}  // namespace
}  // namespace iceberg
