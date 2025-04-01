#include <arrow/filesystem/localfs.h>

#include <cctype>
#include <filesystem>
#include <unordered_set>

#include "gtest/gtest.h"
#include "iceberg/table_metadata.h"
#include "iceberg/uuid.h"
#include "tools/common.h"
#include "tools/metadata_tree.h"

namespace iceberg {
namespace {

bool ValidateUUID(const std::string& uuid) {
  if (uuid.size() != 36) {
    return false;
  }

  static const std::unordered_set<size_t> special_indices{8, 13, 18, 23};
  for (auto ind : special_indices) {
    if (uuid[ind] != '-') {
      return false;
    }
  }

  for (size_t i = 0; i < uuid.size(); ++i) {
    if (special_indices.contains(i)) {
      continue;
    }
    if (!std::isalpha(uuid[i]) && !std::isdigit(uuid[i])) {
      return false;
    }
  }

  return true;
}

bool HasFileWithPrefix(const std::filesystem::path& directory, const std::string& prefix) {
  try {
    if (!std::filesystem::exists(directory) || !std::filesystem::is_directory(directory)) {
      return false;
    }

    for (const auto& entry : std::filesystem::directory_iterator(directory)) {
      if (entry.is_regular_file() && entry.path().filename().string().rfind(prefix, 0) == 0) {
        return true;
      }
    }
  } catch (const std::filesystem::filesystem_error& e) {
    return false;
  }

  return false;
}

std::shared_ptr<TableMetadataV2> DefaultTableMetadata() {
  return std::make_shared<iceberg::TableMetadataV2>(
      std::string{"1"}, std::string{"1"}, 0, 0, 0,
      std::vector<std::shared_ptr<iceberg::Schema>>{
          std::make_shared<iceberg::Schema>(0, std::vector<types::NestedField>{})},
      0,
      std::vector<std::shared_ptr<iceberg::PartitionSpec>>{
          std::make_shared<iceberg::PartitionSpec>(iceberg::PartitionSpec{0, std::vector<iceberg::PartitionField>{}})},
      0, 0, std::map<std::string, std::string>{}, std::nullopt, std::vector<std::shared_ptr<iceberg::Snapshot>>{},
      std::vector<iceberg::SnapshotLog>{}, std::vector<iceberg::MetadataLog>{},
      std::vector<std::shared_ptr<iceberg::SortOrder>>{
          std::make_shared<iceberg::SortOrder>(iceberg::SortOrder{0, std::vector<iceberg::SortField>{}})},
      0, std::map<std::string, iceberg::SnapshotRef>{}, std::vector<iceberg::Statistics>{});
}

}  // namespace

TEST(UUID, Test) {
  Uuid null_uuid;
  EXPECT_TRUE(null_uuid.IsNull());
  EXPECT_EQ(null_uuid.ToString(), "00000000-0000-0000-0000-000000000000");
  EXPECT_TRUE(ValidateUUID(null_uuid.ToString()));

  std::string uuid_str1 = "61f0c404-5cb3-11e7-907b-a6006ad3dba0";
  EXPECT_TRUE(ValidateUUID(uuid_str1));

  Uuid uuid1(uuid_str1);
  EXPECT_FALSE(uuid1.IsNull());
  EXPECT_EQ(uuid1.ToString(), uuid_str1);

  EXPECT_LE(null_uuid, uuid1);
  EXPECT_LT(null_uuid, uuid1);
  EXPECT_GE(uuid1, null_uuid);
  EXPECT_GT(uuid1, null_uuid);
  EXPECT_NE(uuid1, null_uuid);
}

TEST(UUIDGenerator, Test) {
  UuidGenerator generator;

  size_t numUuids = 200;
  std::unordered_set<std::string> seen_uuids;

  for (size_t i = 0; i < numUuids; ++i) {
    auto generated = generator.CreateRandom();
    EXPECT_FALSE(generated.IsNull());

    auto uuid_str = generated.ToString();
    EXPECT_TRUE(ValidateUUID(uuid_str));

    EXPECT_FALSE(seen_uuids.contains(uuid_str));
    seen_uuids.insert(uuid_str);
  }
}

TEST(SnapshotTest, Test) {
  auto metadata_tree = tools::MetadataTree("metadata/00006-8caf3988-3dcc-4ca2-a472-0d96a273eaeb.metadata.json");

  auto snapshot_maker = tools::SnapshotMaker(std::make_shared<arrow::fs::LocalFileSystem>(),
                                             metadata_tree.GetMetadataFile().table_metadata, 0);
  std::shared_ptr<iceberg::TableMetadataV2> table_metadata = DefaultTableMetadata();
  table_metadata->location = "some_location_that_seems_unexpected.json";
  snapshot_maker.table_metadata = table_metadata;

  std::filesystem::create_directory("snapshots");
  std::filesystem::create_directory("data");
  std::filesystem::create_directory("metadata");

  snapshot_maker.MakeMetadataFiles(
      "snapshots", "data", "metadata", "data", {},
      std::vector<std::string>{"data/00000-6-d4e36f4d-a2c0-467d-90e7-0ef1a54e2724-0-00001.parquet"}, {}, 0);

  EXPECT_TRUE(HasFileWithPrefix("snapshots", "snap"));
}

}  // namespace iceberg
