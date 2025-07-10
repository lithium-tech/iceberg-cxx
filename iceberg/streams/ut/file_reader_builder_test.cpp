#include "iceberg/streams/iceberg/file_reader_builder.h"

#include <arrow/status.h>
#include <parquet/arrow/reader.h>

#include "gtest/gtest.h"
#include "iceberg/common/fs/file_reader_provider_impl.h"
#include "iceberg/common/fs/filesystem_provider_impl.h"
#include "iceberg/streams/iceberg/data_entries_meta_stream.h"
#include "iceberg/streams/iceberg/equality_delete_applier.h"
#include "iceberg/streams/iceberg/plan.h"
#include "iceberg/streams/ut/batch_maker.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/column.h"
#include "iceberg/test_utils/optional_vector.h"
#include "iceberg/test_utils/scoped_temp_dir.h"
#include "iceberg/test_utils/write.h"

namespace iceberg {
namespace {

class FileReaderBuilderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    equality_deletes_ = std::make_shared<EqualityDeletes>(EqualityDeletes{});
    fs_provider_ = std::make_shared<FileSystemProvider>(
        std::map<std::string, std::shared_ptr<IFileSystemGetter>>{{"file", std::make_shared<LocalFileSystemGetter>()}});
    data_path_ = "file://" + (dir_.path() / "data.parquet").generic_string();
  }

  std::shared_ptr<EqualityDeletes> equality_deletes_;
  std::shared_ptr<FileSystemProvider> fs_provider_;
  ScopedTempDir dir_;
  std::string data_path_;
};

TEST_F(FileReaderBuilderTest, Trivial) {
  std::vector<int> field_ids_to_retrieve = {1, 3};

  auto field_id_mapper = std::make_shared<FieldIdMapper>(std::map<int, std::string>{{1, "col1"}, {3, "col123"}});

  FileReaderBuilder file_reader_builder(field_ids_to_retrieve, equality_deletes_, field_id_mapper,
                                        std::make_shared<FileReaderProvider>(fs_provider_), nullptr, nullptr);

  auto col1_data = OptionalVector<int64_t>{1, 2, 3, 4};
  auto col2_data = OptionalVector<int32_t>{1, 2, 3, 4};
  auto col3_data = OptionalVector<int32_t>{1, 2, 3, 4};

  auto column1 = MakeInt64Column("xxx", 1, col1_data);
  auto column2 = MakeInt32Column("yyy", 2, col2_data);
  auto column3 = MakeInt32Column("zzz", 3, col2_data);
  ASSERT_OK(WriteToFile({column1, column2, column3}, data_path_));

  PartitionLayerFile state(PartitionLayer(13, 12), data_path_);

  AnnotatedDataPath annotated_data_path(state, {AnnotatedDataPath::Segment{.offset = 4, .length = 0}});

  auto reader = file_reader_builder.Build(annotated_data_path);

  auto batch = reader->ReadNext();
  ASSERT_TRUE(batch);

  EXPECT_EQ(batch->GetPath(), data_path_);
  EXPECT_EQ(batch->GetLayer(), 12);
  EXPECT_EQ(batch->GetPartition(), 13);
}

TEST_F(FileReaderBuilderTest, SchemaNameMapping) {
  std::vector<int> field_ids_to_retrieve = {1, 2};

  auto field_id_mapper = std::make_shared<FieldIdMapper>(std::map<int, std::string>{{1, "col1"}, {2, "col2"}});

  const std::string schema_name_mapping =
      "[ { \"field-id\": 2, \"names\": [\"yyy\"] },"
      " { \"field-id\": 1, \"names\": [\"xxx\"] } ]";

  FileReaderBuilder file_reader_builder(field_ids_to_retrieve, equality_deletes_, field_id_mapper,
                                        std::make_shared<FileReaderProvider>(fs_provider_), nullptr,
                                        &schema_name_mapping);

  auto col1_data = OptionalVector<int32_t>{1, 2, 3, 4};
  auto col2_data = OptionalVector<int32_t>{5, 6, 7, 8};

  auto column1 = MakeInt32Column("xxx", -1, col1_data);
  auto column2 = MakeInt32Column("yyy", -1, col2_data);
  ASSERT_OK(WriteToFile({column1, column2}, data_path_));

  PartitionLayerFile state(PartitionLayer(13, 12), data_path_);

  AnnotatedDataPath annotated_data_path(state, {AnnotatedDataPath::Segment{.offset = 4, .length = 1}});

  auto reader = file_reader_builder.Build(annotated_data_path);

  auto batch = reader->ReadNext();
  ASSERT_TRUE(batch);

  EXPECT_TRUE(batch->GetRecordBatch()->Equals(
      *MakeBatch({MakeInt32ArrowColumn(col1_data), MakeInt32ArrowColumn(col2_data)}, {"col1", "col2"})));
}

}  // namespace
}  // namespace iceberg
