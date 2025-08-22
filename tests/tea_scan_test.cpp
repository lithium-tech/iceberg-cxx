#include "iceberg/tea_scan.h"

#include <arrow/filesystem/filesystem.h>
#include <arrow/filesystem/localfs.h>
#include <arrow/status.h>

#include <exception>
#include <fstream>
#include <stdexcept>
#include <string>

#include "gtest/gtest.h"
#include "iceberg/common/fs/filesystem_wrapper.h"
#include "iceberg/experimental_representations.h"
#include "iceberg/type.h"

namespace iceberg {
namespace {

class ReplacingFilesystem : public FileSystemWrapper {
 public:
  explicit ReplacingFilesystem(std::shared_ptr<arrow::fs::FileSystem> fs) : FileSystemWrapper(fs) {}

  arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenInputFile(const std::string& path) override {
    std::string path_to_use;
    if (path.starts_with("warehouse/")) {
      path_to_use = "tables/" + path.substr(std::string("warehouse/").size());
    } else if (path.starts_with("dl-test-maintenance/check/tea-partitioned/")) {
      path_to_use = "tables/" + path.substr(std::string("dl-test-maintenance/check/tea-partitioned/").size());
    } else {
      return arrow::Status::ExecutionError("Unexpected file prefix in file ", path);
    }
    return FileSystemWrapper::OpenInputFile(path_to_use);
  }
};

template <filter::ValueType value_type>
std::shared_ptr<filter::StatsFilter> MakeGEFilter(const std::string& column_name,
                                                  filter::PhysicalType<value_type> value) {
  auto const_node = std::make_shared<filter::ConstNode>(filter::Value::Make<value_type>(value));
  auto variable_node = std::make_shared<filter::VariableNode>(value_type, column_name);
  filter::FunctionSignature signature = {
      filter::FunctionID::kGreaterThanOrEqualTo, filter::ValueType::kBool, {value_type, value_type}};
  auto final_node = std::make_shared<filter::FunctionNode>(std::move(signature),
                                                           std::vector<filter::NodePtr>{variable_node, const_node});
  return std::make_shared<filter::StatsFilter>(final_node, filter::StatsFilter::Settings{});
}

template <filter::ValueType value_type>
std::shared_ptr<filter::StatsFilter> MakeLEFilter(const std::string& column_name,
                                                  filter::PhysicalType<value_type> value) {
  auto const_node = std::make_shared<filter::ConstNode>(filter::Value::Make<value_type>(value));
  auto variable_node = std::make_shared<filter::VariableNode>(value_type, column_name);
  filter::FunctionSignature signature = {
      filter::FunctionID::kLessThanOrEqualTo, filter::ValueType::kBool, {value_type, value_type}};
  auto final_node = std::make_shared<filter::FunctionNode>(std::move(signature),
                                                           std::vector<filter::NodePtr>{variable_node, const_node});
  return std::make_shared<filter::StatsFilter>(final_node, filter::StatsFilter::Settings{});
}

class PartitionPruningTest : public ::testing::Test {
 protected:
  void SetUp() override {
    fs_ = std::make_shared<arrow::fs::LocalFileSystem>();
    fs_ = std::make_shared<ReplacingFilesystem>(fs_);
  }

  std::shared_ptr<arrow::fs::FileSystem> fs_;
};

TEST(GetScanMetadata, WithPartitionSpecs) {
  std::shared_ptr<arrow::fs::FileSystem> fs = std::make_shared<arrow::fs::LocalFileSystem>();
  fs = std::make_shared<ReplacingFilesystem>(fs);

  struct TestInfo {
    std::string meta_path;
    size_t partitions = 0;
    size_t data_entries = 0;
    size_t delete_entries = 0;
  };

  std::vector<TestInfo> path_to_expected_partitions_count = {
      TestInfo{"s3://warehouse/partitioned_table/metadata/00001-3ac0dc8d-0a8e-44c2-b786-fff45a265023.metadata.json", 6,
               6, 0},
      TestInfo{"s3://warehouse/year_timestamp_partitioning/metadata/"
               "00002-ac56aa65-6214-44b3-bb0f-86728eb58d8b.metadata.json",
               2, 2, 0},
      TestInfo{"s3://warehouse/year_timestamptz_partitioning/metadata/"
               "00002-d52e2c04-065b-4d14-98bb-ec47abcd1597.metadata.json",
               2, 2, 0},
      TestInfo{"s3://warehouse/identity_partitioning/metadata/00002-30bff4d8-0c4f-46a9-8e7a-ebea458dbb1d.metadata.json",
               1, 1, 0},
      TestInfo{"s3://warehouse/bucket_partitioning/metadata/00002-53948f10-cced-409f-8dd9-6dea096895e8.metadata.json",
               1, 1, 0},
      {"s3://warehouse/no_partitioning/metadata/00002-f7dd062a-ad44-4948-ba0c-4cd9f585ba04.metadata.json", 1, 1, 0},
      {"s3://warehouse/partition_evolution/metadata/00005-218c3743-5886-48b9-88d6-86c202862e0f.metadata.json", 7, 7, 0},
      TestInfo{
          "s3://warehouse/year_date_partitioning/metadata/00002-b30c996e-fb0e-4ebc-a987-3536ceb792ea.metadata.json", 2,
          2, 0},
      {"s3://warehouse/month_timestamptz_partitioning/metadata/"
       "00002-f44ed222-470e-4e33-b813-6646d434b185.metadata.json",
       3, 3, 0},
      {"s3://warehouse/day_timestamptz_partitioning/metadata/00002-bf5ed300-c344-41fe-87ad-0d1190705bf9.metadata.json",
       3, 3, 0},
      {"s3://warehouse/hour_timestamptz_partitioning/metadata/00002-aa3a65d9-0e43-452c-a96f-2ec0194f0104.metadata.json",
       3, 3, 0},
      {"s3://warehouse/v_20240913/iceberg/metadata/00001-dcd3b13f-b249-4256-9156-0f653f7da773.metadata.json", 2, 3, 0},
      {"s3://warehouse/prod/db/refdeletes3/metadata/00002-8dbf0bf0-882a-4822-ae9c-ec1c0f34ef6d.metadata.json", 1, 6,
       6}};

  for (const auto& test_info : path_to_expected_partitions_count) {
    for (const bool use_avro_reader_schema : {false, true}) {
      for (const size_t threads_num : {0, 5, 30}) {
        auto maybe_scan_metadata = ice_tea::GetScanMetadata(
            fs, test_info.meta_path, [&](iceberg::Schema& schema) { return use_avro_reader_schema; }, nullptr,
            threads_num);
        ASSERT_EQ(maybe_scan_metadata.status(), arrow::Status::OK()) << test_info.meta_path;
        EXPECT_EQ(maybe_scan_metadata->partitions.size(), test_info.partitions)
            << test_info.meta_path << "\n Threads num: " << threads_num;
        size_t data_entries = 0;
        size_t del_entries = 0;
        for (auto& p : maybe_scan_metadata->partitions) {
          for (auto& l : p) {
            data_entries += l.data_entries_.size();
            del_entries += l.positional_delete_entries_.size();
          }
        }
        EXPECT_EQ(data_entries, test_info.data_entries) << test_info.meta_path;
        EXPECT_EQ(del_entries, test_info.delete_entries) << test_info.meta_path;
      }
    }
  }
}

TEST(GetScanMetadata, WithNoMatchingPartitionSpec) {
  std::shared_ptr<arrow::fs::FileSystem> fs = std::make_shared<arrow::fs::LocalFileSystem>();
  fs = std::make_shared<ReplacingFilesystem>(fs);

  for (const bool use_avro_reader_schema : {false, true}) {
    for (const size_t threads_num : {0, 5, 30}) {
      ASSERT_ANY_THROW(ice_tea::GetScanMetadata(
                           fs,
                           "s3://warehouse/partitioned_table_with_missing_spec/metadata/"
                           "00001-3ac0dc8d-0a8e-44c2-b786-fff45a265023.metadata.json",
                           [&](iceberg::Schema& schema) { return use_avro_reader_schema; }, nullptr, threads_num)
                           .ok());
    }
  }
}

// TODO(gmusya): this test is broken and misleading (it is a copy of WithNoMatchingPartitionSpec test), fix
#if 0
TEST(GetScanMetadata, WithVoidInPartitionSpec) {
  std::shared_ptr<arrow::fs::FileSystem> fs = std::make_shared<arrow::fs::LocalFileSystem>();
  fs = std::make_shared<ReplacingFilesystem>(fs);

  auto maybe_scan_metadata = ice_tea::GetScanMetadata(fs,
                                                      "s3://warehouse/partitioned_table_with_missing_spec/metadata/"
                                                      "00001-3ac0dc8d-0a8e-44c2-b786-fff45a265023.metadata.json");
  ASSERT_NE(maybe_scan_metadata.status(), arrow::Status::OK());
  std::string error_message = maybe_scan_metadata.status().message();

  // null value was expected for void transform
  EXPECT_EQ(error_message,
            "Partiton specification for entry "
            "s3a://warehouse/partitioned_table/data/c1=2/c2=2025-03-04/"
            "20250303_133349_00017_es78y-ab06c0f6-2a0b-46c9-b42e-dd27880eb385.parquet is not found");
}
#endif

TEST(GetScanMetadata, WithMultipleMatchingPartitionSpecs) {
  std::shared_ptr<arrow::fs::FileSystem> fs = std::make_shared<arrow::fs::LocalFileSystem>();
  fs = std::make_shared<ReplacingFilesystem>(fs);

  try {
    for (const bool use_avro_reader_schema : {false, true}) {
      for (const size_t threads_num : {0, 5, 30}) {
        auto maybe_scan_metadata = ice_tea::GetScanMetadata(
            fs,
            "s3://warehouse/partitioned_table_with_multiple_spec/metadata/"
            "00001-3ac0dc8d-0a8e-44c2-b786-fff45a265023.metadata.json",
            [&](iceberg::Schema& schema) { return use_avro_reader_schema; }, nullptr, threads_num);
        ASSERT_NE(maybe_scan_metadata.status(), arrow::Status::OK());
        std::string error_message = maybe_scan_metadata.status().message();

        EXPECT_EQ(error_message,
                  "Multiple (2) partiton specifications for entry "
                  "s3a://warehouse/partitioned_table/data/c1=2/c2=2025-03-04/"
                  "20250303_133349_00017_es78y-ab06c0f6-2a0b-46c9-b42e-dd27880eb385.parquet are found");
      }
    }
  } catch (std::exception& e) {
    ASSERT_TRUE(false) << e.what() << std::endl;
  } catch (arrow::Status& e) {
    ASSERT_TRUE(false) << e.ToString() << std::endl;
  }
}

TEST(GetScanMetadata, EqualityDataEntries) {
  ice_tea::DataEntry data_entry1(
      "a", {ice_tea::DataEntry::Segment(3, 5), ice_tea::DataEntry::Segment(10, 3), ice_tea::DataEntry::Segment(13, 2)});
  ice_tea::DataEntry data_entry2(
      "a", {ice_tea::DataEntry::Segment(3, 2), ice_tea::DataEntry::Segment(5, 3), ice_tea::DataEntry::Segment(10, 5)});
  ice_tea::DataEntry data_entry3("a", {ice_tea::DataEntry::Segment(3, 2), ice_tea::DataEntry::Segment(5, 8)});

  EXPECT_TRUE(experimental::AreDataEntriesEqual(data_entry1, data_entry2));
  EXPECT_FALSE(experimental::AreDataEntriesEqual(data_entry1, data_entry3));
  EXPECT_FALSE(experimental::AreDataEntriesEqual(data_entry2, data_entry3));

  ice_tea::DataEntry data_entry4("a", {ice_tea::DataEntry::Segment(3, 0)});
  ice_tea::DataEntry data_entry5("a", {ice_tea::DataEntry::Segment(3, 2), ice_tea::DataEntry::Segment(5, 0)});

  EXPECT_TRUE(experimental::AreDataEntriesEqual(data_entry4, data_entry5));
}

TEST(GetScanMetadata, EqualityScanMetadataWithoutDeletes) {
  ice_tea::ScanMetadata scan_meta1;
  ice_tea::ScanMetadata scan_meta2;
  ice_tea::ScanMetadata scan_meta3;
  ice_tea::ScanMetadata scan_meta4;

  ice_tea::DataEntry data_entry1("a");
  ice_tea::DataEntry data_entry2("b");
  ice_tea::DataEntry data_entry3("c");

  scan_meta1.partitions.push_back(
      ice_tea::ScanMetadata::Partition{ice_tea::ScanMetadata::Layer{.data_entries_ = std::vector{data_entry1}}});
  scan_meta1.partitions.push_back(
      ice_tea::ScanMetadata::Partition{ice_tea::ScanMetadata::Layer{.data_entries_ = std::vector{data_entry2}}});
  scan_meta1.partitions.push_back(
      ice_tea::ScanMetadata::Partition{ice_tea::ScanMetadata::Layer{.data_entries_ = std::vector{data_entry3}}});

  scan_meta2.partitions.push_back(ice_tea::ScanMetadata::Partition{
      ice_tea::ScanMetadata::Layer{.data_entries_ = std::vector{data_entry1}},
      ice_tea::ScanMetadata::Layer{.data_entries_ = std::vector{data_entry2}},
      ice_tea::ScanMetadata::Layer{.data_entries_ = std::vector{data_entry3}},
  });

  scan_meta3.partitions.push_back(ice_tea::ScanMetadata::Partition{
      ice_tea::ScanMetadata::Layer{.data_entries_ = std::vector{data_entry1, data_entry2, data_entry3}},
  });

  scan_meta4.partitions.push_back(ice_tea::ScanMetadata::Partition{
      ice_tea::ScanMetadata::Layer{.data_entries_ = std::vector{data_entry1, data_entry2}},
  });

  EXPECT_TRUE(experimental::AreScanMetadataEqual(scan_meta1, scan_meta1));
  EXPECT_TRUE(experimental::AreScanMetadataEqual(scan_meta1, scan_meta2));
  EXPECT_TRUE(experimental::AreScanMetadataEqual(scan_meta1, scan_meta3));
  EXPECT_FALSE(experimental::AreScanMetadataEqual(scan_meta1, scan_meta4));

  EXPECT_TRUE(experimental::AreScanMetadataEqual(scan_meta2, scan_meta2));
  EXPECT_TRUE(experimental::AreScanMetadataEqual(scan_meta2, scan_meta3));
  EXPECT_FALSE(experimental::AreScanMetadataEqual(scan_meta2, scan_meta4));

  EXPECT_TRUE(experimental::AreScanMetadataEqual(scan_meta3, scan_meta3));
  EXPECT_FALSE(experimental::AreScanMetadataEqual(scan_meta3, scan_meta4));

  EXPECT_TRUE(experimental::AreScanMetadataEqual(scan_meta4, scan_meta4));
}

TEST(GetScanMetadata, EqualityScanMetadataWithDeletes) {
  ice_tea::ScanMetadata scan_meta1;
  ice_tea::ScanMetadata scan_meta2;
  ice_tea::ScanMetadata scan_meta3;
  ice_tea::ScanMetadata scan_meta4;

  ice_tea::DataEntry data_entry1("a");
  ice_tea::DataEntry data_entry2("b");
  ice_tea::DataEntry data_entry3("c");
  ice_tea::DataEntry data_entry4("c");

  ice_tea::PositionalDeleteInfo pos_delete1("p_a");

  ice_tea::EqualityDeleteInfo eq_delete1("eq_a", {});

  scan_meta1.partitions.push_back(ice_tea::ScanMetadata::Partition{
      ice_tea::ScanMetadata::Layer{.data_entries_ = std::vector{data_entry1}},
      ice_tea::ScanMetadata::Layer{.data_entries_ = std::vector{data_entry2, data_entry3, data_entry4}},
      ice_tea::ScanMetadata::Layer{.equality_delete_entries_ = std::vector{eq_delete1}},
  });

  scan_meta2.partitions.push_back(ice_tea::ScanMetadata::Partition{
      ice_tea::ScanMetadata::Layer{.data_entries_ = std::vector{data_entry1},
                                   .equality_delete_entries_ = std::vector{eq_delete1}},
      ice_tea::ScanMetadata::Layer{.data_entries_ = std::vector{data_entry2, data_entry3, data_entry4}},
      ice_tea::ScanMetadata::Layer{.positional_delete_entries_ = std::vector{pos_delete1}},
  });

  scan_meta3.partitions.push_back(ice_tea::ScanMetadata::Partition{
      ice_tea::ScanMetadata::Layer{.equality_delete_entries_ = std::vector{eq_delete1}},
      ice_tea::ScanMetadata::Layer{.data_entries_ = std::vector{data_entry3, data_entry1}},
      ice_tea::ScanMetadata::Layer{.data_entries_ = std::vector{data_entry4, data_entry2},
                                   .positional_delete_entries_ = std::vector{pos_delete1}},
  });

  scan_meta4.partitions.push_back(ice_tea::ScanMetadata::Partition{
      ice_tea::ScanMetadata::Layer{.equality_delete_entries_ = std::vector{eq_delete1}},
      ice_tea::ScanMetadata::Layer{.data_entries_ = std::vector{data_entry3, data_entry1}},
      ice_tea::ScanMetadata::Layer{.positional_delete_entries_ = std::vector{pos_delete1}},
      ice_tea::ScanMetadata::Layer{.data_entries_ = std::vector{data_entry4, data_entry2}},
  });

  EXPECT_TRUE(experimental::AreScanMetadataEqual(iceberg::ice_tea::ScanMetadata{}, iceberg::ice_tea::ScanMetadata{}));

  EXPECT_FALSE(experimental::AreScanMetadataEqual(scan_meta1, scan_meta2));
  EXPECT_FALSE(experimental::AreScanMetadataEqual(scan_meta1, scan_meta3));
  EXPECT_FALSE(experimental::AreScanMetadataEqual(scan_meta2, scan_meta3));

  EXPECT_TRUE(experimental::AreScanMetadataEqual(scan_meta3, scan_meta4));
}

TEST(GetScanMetadata, EqualityScanMetadataWithMultipleSegments) {
  ice_tea::ScanMetadata scan_meta1;
  ice_tea::ScanMetadata scan_meta2;
  ice_tea::ScanMetadata scan_meta3;

  ice_tea::DataEntry data_entry1("a", {ice_tea::DataEntry::Segment(3, 5), ice_tea::DataEntry::Segment(10, 5)});
  ice_tea::DataEntry data_entry2("a", {ice_tea::DataEntry::Segment(3, 5)});
  ice_tea::DataEntry data_entry3("a", {ice_tea::DataEntry::Segment(10, 5)});

  scan_meta1.partitions.push_back(
      ice_tea::ScanMetadata::Partition{ice_tea::ScanMetadata::Layer{.data_entries_ = std::vector{data_entry1}}});

  scan_meta2.partitions.push_back(ice_tea::ScanMetadata::Partition{
      ice_tea::ScanMetadata::Layer{.data_entries_ = std::vector{data_entry2}},
      ice_tea::ScanMetadata::Layer{.data_entries_ = std::vector{data_entry3}},
  });

  scan_meta3.partitions.push_back(ice_tea::ScanMetadata::Partition{
      ice_tea::ScanMetadata::Layer{.data_entries_ = std::vector{data_entry1, data_entry2}},
  });

  EXPECT_TRUE(experimental::AreScanMetadataEqual(scan_meta1, scan_meta2));
  EXPECT_TRUE(experimental::AreScanMetadataEqual(scan_meta1, scan_meta3));
}

TEST_F(PartitionPruningTest, YearTimestamp) {
  std::ifstream input(
      "tables/year_timestamp_partitioning/metadata/"
      "00002-ac56aa65-6214-44b3-bb0f-86728eb58d8b.metadata.json");
  auto metadata = ice_tea::ReadTableMetadataV2(input);

  {
    auto filterGE = MakeGEFilter<filter::ValueType::kTimestamp>("c2", 1767225600000000ll);  // 2026.01.01 00:00:00
    auto streamGE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterGE);
    {
      auto maybe_manifest_entry = streamGE->ReadNext();
      EXPECT_FALSE(maybe_manifest_entry.has_value());
    }
  }

  {
    auto filterGE = MakeGEFilter<filter::ValueType::kTimestamp>(
        "c2", 1767225600000000ll - 1);  // microsecond before 2026.01.01 00:00:00
    auto streamGE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterGE);
    {
      auto maybe_manifest_entry = streamGE->ReadNext();
      EXPECT_TRUE(maybe_manifest_entry.has_value());
    }
  }

  {
    auto filterLE = MakeLEFilter<filter::ValueType::kTimestamp>(
        "c2", 1704067200000000ll - 1);  // microsecond before 2024.01.01 00:00:00
    auto streamLE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterLE);
    {
      auto maybe_manifest_entry = streamLE->ReadNext();
      EXPECT_FALSE(maybe_manifest_entry.has_value());
    }
  }

  {
    auto filterLE = MakeLEFilter<filter::ValueType::kTimestamp>("c2", 1704067200000000ll);  //  2024.01.01 00:00:00
    auto streamLE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterLE);
    {
      auto maybe_manifest_entry = streamLE->ReadNext();
      EXPECT_TRUE(maybe_manifest_entry.has_value());
    }
  }
}

constexpr uint64_t kMicrosInDay = 24ll * 60 * 60 * 1000 * 1000;

TEST_F(PartitionPruningTest, YearTimestamptz) {
  std::ifstream input(
      "tables/year_timestamptz_partitioning/metadata/"
      "00002-d52e2c04-065b-4d14-98bb-ec47abcd1597.metadata.json");
  auto metadata = ice_tea::ReadTableMetadataV2(input);

  {
    auto filterGE =
        MakeGEFilter<filter::ValueType::kTimestamptz>("c2", 1767225600000000ll + kMicrosInDay);  // 2026.01.02 00:00:00
    auto streamGE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterGE);
    {
      auto maybe_manifest_entry = streamGE->ReadNext();
      EXPECT_FALSE(maybe_manifest_entry.has_value());
    }
  }
  {
    auto filterGE = MakeGEFilter<filter::ValueType::kTimestamptz>(
        "c2", 1767225600000000ll + kMicrosInDay - 1);  // microsecond before 2026.01.02 00:00:00
    auto streamGE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterGE);
    {
      auto maybe_manifest_entry = streamGE->ReadNext();
      EXPECT_TRUE(maybe_manifest_entry.has_value());
    }
  }

  {
    auto filterLE = MakeLEFilter<filter::ValueType::kTimestamptz>(
        "c2", 1704067200000000ll - kMicrosInDay - 1);  // microsecond before 2023.12.31 00:00:00
    auto streamLE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterLE);
    {
      auto maybe_manifest_entry = streamLE->ReadNext();
      EXPECT_FALSE(maybe_manifest_entry.has_value());
    }
  }
  {
    auto filterLE =
        MakeLEFilter<filter::ValueType::kTimestamptz>("c2", 1704067200000000ll - kMicrosInDay);  // 2023.12.31 00:00:00
    auto streamLE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterLE);
    {
      auto maybe_manifest_entry = streamLE->ReadNext();
      EXPECT_TRUE(maybe_manifest_entry.has_value());
    }
  }
}

TEST_F(PartitionPruningTest, MonthTimestamptz) {
  std::ifstream input(
      "tables/month_timestamptz_partitioning/metadata/00002-f44ed222-470e-4e33-b813-6646d434b185.metadata.json");
  auto metadata = ice_tea::ReadTableMetadataV2(input);

  {
    auto filterGE =
        MakeGEFilter<filter::ValueType::kTimestamptz>("c2", 1743465600000000ll + kMicrosInDay);  // 2025.04.02 00:00:00
    auto streamGE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterGE);
    {
      auto maybe_manifest_entry = streamGE->ReadNext();
      EXPECT_FALSE(maybe_manifest_entry.has_value());
    }
  }
  {
    auto filterGE = MakeGEFilter<filter::ValueType::kTimestamptz>(
        "c2", 1743465600000000ll + kMicrosInDay - 1);  // microsecond before 2025.04.02 00:00:00
    auto streamGE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterGE);
    {
      auto maybe_manifest_entry = streamGE->ReadNext();
      EXPECT_TRUE(maybe_manifest_entry.has_value());
    }
  }

  {
    auto filterLE = MakeLEFilter<filter::ValueType::kTimestamptz>(
        "c2", 1709251200000000 - kMicrosInDay - 1);  // microsecond before 2024.02.28 00:00:00
    auto streamLE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterLE);
    {
      auto maybe_manifest_entry = streamLE->ReadNext();
      EXPECT_FALSE(maybe_manifest_entry.has_value());
    }
  }
  {
    auto filterLE =
        MakeLEFilter<filter::ValueType::kTimestamptz>("c2", 1709251200000000 - kMicrosInDay);  // 2024.02.28 00:00:00
    auto streamLE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterLE);
    {
      auto maybe_manifest_entry = streamLE->ReadNext();
      EXPECT_TRUE(maybe_manifest_entry.has_value());
    }
  }
}

TEST_F(PartitionPruningTest, DayTimestamptz) {
  std::ifstream input(
      "tables/day_timestamptz_partitioning/metadata/00002-bf5ed300-c344-41fe-87ad-0d1190705bf9.metadata.json");
  auto metadata = ice_tea::ReadTableMetadataV2(input);

  {
    auto filterGE =
        MakeGEFilter<filter::ValueType::kTimestamptz>("c2", 1741046400000000ll + kMicrosInDay);  // 2025.03.05 00:00:00
    auto streamGE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterGE);
    {
      auto maybe_manifest_entry = streamGE->ReadNext();
      EXPECT_FALSE(maybe_manifest_entry.has_value());
    }
  }
  {
    auto filterGE = MakeGEFilter<filter::ValueType::kTimestamptz>(
        "c2", 1741046400000000ll + kMicrosInDay - 1);  // microsecond before 2025.03.05 00:00:00
    auto streamGE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterGE);
    {
      auto maybe_manifest_entry = streamGE->ReadNext();
      EXPECT_TRUE(maybe_manifest_entry.has_value());
    }
  }
  {
    auto filterLE = MakeLEFilter<filter::ValueType::kTimestamptz>(
        "c2", 1709424000000000ll - kMicrosInDay - 1);  // microsecond before 2024.03.03 00:00:00
    auto streamLE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterLE);
    {
      auto maybe_manifest_entry = streamLE->ReadNext();
      EXPECT_FALSE(maybe_manifest_entry.has_value());
    }
  }
  {
    auto filterLE =
        MakeLEFilter<filter::ValueType::kTimestamptz>("c2", 1709424000000000ll - kMicrosInDay);  // 2024.03.03 00:00:00
    auto streamLE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterLE);
    {
      auto maybe_manifest_entry = streamLE->ReadNext();
      EXPECT_TRUE(maybe_manifest_entry.has_value());
    }
  }
}

TEST_F(PartitionPruningTest, HourTimestamptz) {
  std::ifstream input(
      "tables/hour_timestamptz_partitioning/metadata/00002-aa3a65d9-0e43-452c-a96f-2ec0194f0104.metadata.json");
  auto metadata = ice_tea::ReadTableMetadataV2(input);

  {
    auto filterGE = MakeGEFilter<filter::ValueType::kTimestamptz>(
        "c2", 1741046400000000ll + kMicrosInDay);  // microsecond before 2025.03.05 00:00:00
    auto streamGE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterGE);
    {
      auto maybe_manifest_entry = streamGE->ReadNext();
      EXPECT_FALSE(maybe_manifest_entry.has_value());
    }
  }
  {
    auto filterGE = MakeGEFilter<filter::ValueType::kTimestamptz>(
        "c2", 1741046400000000ll + kMicrosInDay - 1);  // 2025.03.05 00:00:00
    auto streamGE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterGE);
    {
      auto maybe_manifest_entry = streamGE->ReadNext();
      EXPECT_TRUE(maybe_manifest_entry.has_value());
    }
  }

  {
    auto filterLE = MakeLEFilter<filter::ValueType::kTimestamptz>(
        "c2", 1709506800000000ll - kMicrosInDay - 1);  // microsecond before 2023.03.02 23:00:00
    auto streamLE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterLE);
    {
      auto maybe_manifest_entry = streamLE->ReadNext();
      EXPECT_FALSE(maybe_manifest_entry.has_value());
    }
  }
  {
    auto filterLE =
        MakeLEFilter<filter::ValueType::kTimestamptz>("c2", 1709506800000000ll - kMicrosInDay);  //  2023.03.02 23:00:00
    auto streamLE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterLE);
    {
      auto maybe_manifest_entry = streamLE->ReadNext();
      EXPECT_TRUE(maybe_manifest_entry.has_value());
    }
  }
}

TEST_F(PartitionPruningTest, Identity) {
  std::ifstream input("tables/identity_partitioning/metadata/00002-30bff4d8-0c4f-46a9-8e7a-ebea458dbb1d.metadata.json");
  auto metadata = ice_tea::ReadTableMetadataV2(input);

  auto filterGE = MakeGEFilter<filter::ValueType::kDate>("col_date", 20151);  // 2025.03.04
  auto streamGE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterGE);
  {
    auto maybe_manifest_entry = streamGE->ReadNext();
    EXPECT_FALSE(maybe_manifest_entry.has_value());
  }

  auto filterLE = MakeLEFilter<filter::ValueType::kString>("col_string", "rome-string");
  auto streamLE = ice_tea::AllEntriesStream::Make(fs_, metadata, false, filterLE);
  {
    auto maybe_manifest_entry = streamLE->ReadNext();
    EXPECT_FALSE(maybe_manifest_entry.has_value());
  }
}

}  // namespace
}  // namespace iceberg
