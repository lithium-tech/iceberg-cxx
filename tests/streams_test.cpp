#include <fstream>

#include "arrow/util/logging.h"
#include "gtest/gtest.h"
#include "iceberg/common/fs/file_reader_provider_impl.h"
#include "iceberg/common/fs/filesystem_provider_impl.h"
#include "iceberg/literals.h"
#include "iceberg/streams/iceberg/builder.h"
#include "iceberg/streams/ut/batch_maker.h"
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

  auto entries_stream = ice_tea::AllEntriesStream::Make(fs, metadata, false);
  Ensure(!!entries_stream, "Failed to make AllEntriesStream");

  auto maybe_scan_metadata = ice_tea::GetScanMetadata(*entries_stream, *metadata, nullptr);
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
      nullptr, nullptr, *metadata->GetCurrentSchema(), field_ids_to_retrieve,
      std::make_shared<FileReaderProvider>(fs_provider), schema_name_mapping);
}

std::shared_ptr<arrow::Scalar> CreateStringListScalar(const std::vector<std::string>& values) {
  arrow::StringBuilder string_builder;
  ARROW_CHECK_OK(string_builder.AppendValues(values));

  std::shared_ptr<arrow::Array> values_array;
  ARROW_CHECK_OK(string_builder.Finish(&values_array));

  auto list_type = arrow::list(arrow::utf8());
  return std::make_shared<arrow::ListScalar>(values_array, list_type);
}

void CheckSecondRecord(IcebergStreamPtr data_stream) {
  auto batch = data_stream->ReadNext();
  ASSERT_TRUE(!!batch);

  auto record_batch = batch->GetRecordBatch();
  ASSERT_TRUE(!!record_batch);

  EXPECT_TRUE(record_batch->GetColumnByName("array_col")
                  ->Equals(*Literal(CreateStringListScalar({"item1", "item2"})).MakeColumn(1)));
  EXPECT_TRUE(record_batch->GetColumnByName("binary_col")
                  ->Equals(*Literal(std::make_shared<arrow::BinaryScalar>(
                                        arrow::Buffer::FromString("\x73\x70\x61\x72\x6b"), arrow::binary()))
                                .MakeColumn(1)));
  EXPECT_TRUE(record_batch->GetColumnByName("boolean_col")
                  ->Equals(*Literal(std::make_shared<arrow::BooleanScalar>(true)).MakeColumn(1)));
  EXPECT_TRUE(record_batch->GetColumnByName("date_col")
                  ->Equals(*Literal(std::make_shared<arrow::Date32Scalar>(19640)).MakeColumn(1)));
  EXPECT_TRUE(record_batch->GetColumnByName("decimal_col")
                  ->Equals(*Literal(std::make_shared<arrow::Decimal128Scalar>(
                                        arrow::Decimal128::FromString("12345.67").ValueOrDie(),
                                        std::make_shared<arrow::Decimal128Type>(10, 2)))
                                .MakeColumn(1)));
  EXPECT_TRUE(record_batch->GetColumnByName("double_col")
                  ->Equals(*Literal(std::make_shared<arrow::DoubleScalar>(1.7976931348623157e308)).MakeColumn(1)));
  EXPECT_TRUE(record_batch->GetColumnByName("float_col")
                  ->Equals(*Literal(std::make_shared<arrow::FloatScalar>(3.4028235e+38)).MakeColumn(1)));
  EXPECT_TRUE(
      record_batch->GetColumnByName("id")->Equals(*Literal(std::make_shared<arrow::Int64Scalar>(2)).MakeColumn(1)));
  EXPECT_TRUE(record_batch->GetColumnByName("integer_col")
                  ->Equals(*Literal(std::make_shared<arrow::Int32Scalar>(2147483647)).MakeColumn(1)));
  EXPECT_TRUE(record_batch->GetColumnByName("long_col")
                  ->Equals(*Literal(std::make_shared<arrow::Int64Scalar>(9223372036854775807)).MakeColumn(1)));
  EXPECT_TRUE(record_batch->GetColumnByName("string_col")
                  ->Equals(*Literal(std::make_shared<arrow::StringScalar>("example")).MakeColumn(1)));
  EXPECT_TRUE(record_batch->GetColumnByName("timestamp_col")
                  ->Equals(*Literal(std::make_shared<arrow::TimestampScalar>(1696941296000000, arrow::TimeUnit::MICRO))
                                .MakeColumn(1)));
  EXPECT_TRUE(
      record_batch->GetColumnByName("timestamptz_col")
          ->Equals(*Literal(std::make_shared<arrow::TimestampScalar>(1696941296000000, arrow::TimeUnit::MICRO, "UTC"))
                        .MakeColumn(1)));
}

TEST(StreamsTest, EndToEndNullColumn) {
  const std::string path =
      "warehouse/streams/default_value/metadata/00014-c179c339-e10a-486b-b075-144f5e5b101a.metadata.json";
  std::vector<int> field_ids_to_retrieve(13);
  std::iota(field_ids_to_retrieve.begin(), field_ids_to_retrieve.end(), 1);
  auto data_stream = MakeDataStream(path, field_ids_to_retrieve);

  CheckSecondRecord(data_stream);

  auto batch = data_stream->ReadNext();
  ASSERT_TRUE(!!batch);

  auto record_batch = batch->GetRecordBatch();
  ASSERT_TRUE(!!record_batch);

  EXPECT_TRUE(
      record_batch->GetColumnByName("id")->Equals(*Literal(std::make_shared<arrow::Int64Scalar>(1)).MakeColumn(1)));

  batch = data_stream->ReadNext();
  ASSERT_TRUE(!batch);
}

TEST(StreamsTest, EndToEndWithDefaultValue) {
  const std::string path = "warehouse/streams/default_value/metadata/DefaultValues.json";
  std::vector<int> field_ids_to_retrieve(13);
  std::iota(field_ids_to_retrieve.begin(), field_ids_to_retrieve.end(), 1);
  auto data_stream = MakeDataStream(path, field_ids_to_retrieve);

  CheckSecondRecord(data_stream);

  auto batch = data_stream->ReadNext();
  ASSERT_TRUE(!!batch);

  auto record_batch = batch->GetRecordBatch();
  ASSERT_TRUE(!!record_batch);

  EXPECT_TRUE(
      record_batch->GetColumnByName("array_col")->Equals(*Literal(CreateStringListScalar({"b", "fwl"})).MakeColumn(1)));
  EXPECT_TRUE(record_batch->GetColumnByName("binary_col")
                  ->Equals(*Literal(std::make_shared<arrow::BinaryScalar>(
                                        arrow::Buffer::FromString("\xb5\xb5\xb5\xb5\xb5"), arrow::binary()))
                                .MakeColumn(1)));
  EXPECT_TRUE(record_batch->GetColumnByName("boolean_col")
                  ->Equals(*Literal(std::make_shared<arrow::BooleanScalar>(true)).MakeColumn(1)));
  EXPECT_TRUE(record_batch->GetColumnByName("date_col")
                  ->Equals(*Literal(std::make_shared<arrow::Date32Scalar>(1039)).MakeColumn(1)));
  EXPECT_TRUE(record_batch->GetColumnByName("decimal_col")
                  ->Equals(*Literal(std::make_shared<arrow::Decimal128Scalar>(
                                        arrow::Decimal128::FromString("3.14").ValueOrDie(),
                                        std::make_shared<arrow::Decimal128Type>(10, 2)))
                                .MakeColumn(1)));
  EXPECT_TRUE(record_batch->GetColumnByName("double_col")
                  ->Equals(*Literal(std::make_shared<arrow::DoubleScalar>(6.21)).MakeColumn(1)));
  EXPECT_TRUE(record_batch->GetColumnByName("float_col")
                  ->Equals(*Literal(std::make_shared<arrow::FloatScalar>(3.5)).MakeColumn(1)));
  EXPECT_TRUE(
      record_batch->GetColumnByName("id")->Equals(*Literal(std::make_shared<arrow::Int64Scalar>(1)).MakeColumn(1)));
  EXPECT_TRUE(record_batch->GetColumnByName("integer_col")
                  ->Equals(*Literal(std::make_shared<arrow::Int32Scalar>(17)).MakeColumn(1)));
  EXPECT_TRUE(record_batch->GetColumnByName("long_col")
                  ->Equals(*Literal(std::make_shared<arrow::Int64Scalar>(9)).MakeColumn(1)));
  EXPECT_TRUE(record_batch->GetColumnByName("string_col")
                  ->Equals(*Literal(std::make_shared<arrow::StringScalar>("default")).MakeColumn(1)));
  EXPECT_TRUE(record_batch->GetColumnByName("timestamp_col")
                  ->Equals(*Literal(std::make_shared<arrow::TimestampScalar>(1351728650051000, arrow::TimeUnit::MICRO))
                                .MakeColumn(1)));
  EXPECT_TRUE(
      record_batch->GetColumnByName("timestamptz_col")
          ->Equals(*Literal(std::make_shared<arrow::TimestampScalar>(1510871468123456, arrow::TimeUnit::MICRO, "UTC"))
                        .MakeColumn(1)));

  batch = data_stream->ReadNext();
  ASSERT_TRUE(!batch);
}

}  // namespace
}  // namespace iceberg
