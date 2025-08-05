#include "iceberg/streams/iceberg/file_reader_builder.h"

#include <arrow/status.h>

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

TEST(FileReaderBuilder, Trivial) {
  auto equality_deletes = std::make_shared<EqualityDeletes>(EqualityDeletes{});

  std::vector<int> field_ids_to_retrieve = {1, 3};

  auto field_id_mapper = std::make_shared<FieldIdMapper>(std::map<int, std::string>{{1, "col1"}, {3, "col123"}});

  auto fs_provider = std::make_shared<FileSystemProvider>(
      std::map<std::string, std::shared_ptr<IFileSystemGetter>>{{"file", std::make_shared<LocalFileSystemGetter>()}});

  FileReaderBuilder file_reader_builder(field_ids_to_retrieve, equality_deletes, field_id_mapper,
                                        std::make_shared<FileReaderProvider>(fs_provider), nullptr, nullptr);

  ScopedTempDir dir;
  std::string data_path = "file://" + (dir.path() / "data.parquet").generic_string();

  auto col1_data = OptionalVector<int64_t>{1, 2, 3, 4};
  auto col2_data = OptionalVector<int32_t>{1, 2, 3, 4};
  auto col3_data = OptionalVector<int32_t>{1, 2, 3, 4};

  auto column1 = MakeInt64Column("xxx", 1, col1_data);
  auto column2 = MakeInt32Column("yyy", 2, col2_data);
  auto column3 = MakeInt32Column("zzz", 3, col2_data);
  ASSERT_OK(WriteToFile({column1, column2, column3}, data_path));

  PartitionLayerFile state(PartitionLayer(13, 12), data_path);

  AnnotatedDataPath annotated_data_path(state, {AnnotatedDataPath::Segment{.offset = 4, .length = 0}});

  auto reader = file_reader_builder.Build(annotated_data_path);

  auto batch = reader->ReadNext();
  ASSERT_TRUE(batch);

  EXPECT_EQ(batch->GetPath(), data_path);
  EXPECT_EQ(batch->GetLayer(), 12);
  EXPECT_EQ(batch->GetPartition(), 13);
}

TEST(FileReaderBuilder, RowFilter) {
  auto equality_deletes = std::make_shared<EqualityDeletes>(EqualityDeletes{});

  std::vector<int> field_ids_to_retrieve = {1, 3};

  auto field_id_mapper =
      std::make_shared<FieldIdMapper>(std::map<int, std::string>{{1, "col1"}, {2, "col2"}, {3, "col3"}});

  auto fs_provider = std::make_shared<FileSystemProvider>(
      std::map<std::string, std::shared_ptr<IFileSystemGetter>>{{"file", std::make_shared<LocalFileSystemGetter>()}});

  auto row_filter = std::make_shared<PassThroughFilter>(std::vector<int32_t>{3, 2});

  FileReaderBuilder file_reader_builder(field_ids_to_retrieve, equality_deletes, field_id_mapper,
                                        std::make_shared<FileReaderProvider>(fs_provider), nullptr, row_filter);

  ScopedTempDir dir;
  std::string data_path = "file://" + (dir.path() / "data.parquet").generic_string();

  auto col1_data = OptionalVector<int32_t>{1, 2, 3, 4};
  auto col2_data = OptionalVector<int32_t>{5, 6, 7, 8};
  auto col3_data = OptionalVector<int32_t>{9, 10, 11, 12};

  auto column1 = MakeInt16Column("xxx", 1, col1_data);
  auto column2 = MakeInt32Column("yyy", 2, col2_data);
  auto column3 = MakeInt32Column("zzz", 3, col3_data);
  ASSERT_OK(WriteToFile({column1, column2, column3}, data_path));

  PartitionLayerFile state(PartitionLayer(13, 12), data_path);

  AnnotatedDataPath annotated_data_path(state, {AnnotatedDataPath::Segment{.offset = 4, .length = 1}});

  auto reader = file_reader_builder.Build(annotated_data_path);

  auto batch = reader->ReadNext();
  ASSERT_TRUE(batch);

  EXPECT_EQ(batch->GetRecordBatch()->num_columns(), 3);
  EXPECT_TRUE(batch->GetRecordBatch()->GetColumnByName("col1")->Equals(*MakeInt16ArrowColumn({1, 2, 3, 4})));
  EXPECT_TRUE(batch->GetRecordBatch()->GetColumnByName("col2")->Equals(*MakeInt32ArrowColumn({5, 6, 7, 8})));
  EXPECT_TRUE(batch->GetRecordBatch()->GetColumnByName("col3")->Equals(*MakeInt32ArrowColumn({9, 10, 11, 12})));
}

}  // namespace
}  // namespace iceberg
