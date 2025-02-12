#include "tools/validation.h"

#include <fstream>

#include "gtest/gtest.h"

namespace iceberg {

namespace {
Manifest ReadManifest(const std::string &path) {
  std::ifstream input(path);
  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();
  return ice_tea::ReadManifestEntries(data);
}

tools::Restrictions DefaultRestictions() {
  return {1000,
          1000,
          1000,
          1000,
          1000,
          1,
          90,
          10,
          100000,
          1,
          10000000,
          {"4412d001-c6df-4adb-8854-d3b9e762440c"},
          {"4412d001-c6df-4adb-8854-d3b9e762440c"},
          {"main"},
          {"min", "max"},
          {{"owner", "root"}, {"created-at", std::nullopt}, {"write.parquet.compression-codec", "zstd"}},
          {TypeID::kBinary}};
}

std::vector<std::shared_ptr<Manifest>> DefaultManifests() {
  return {std::make_shared<Manifest>(ReadManifest("metadata/7e6e13cb-31fd-4de7-8811-02ce7cec44a9-m0.avro")),
          std::make_shared<Manifest>(ReadManifest("metadata/02ce7cec-31fd-4de7-8811-02ce7cec44a9-m0.avro")),
          std::make_shared<Manifest>(ReadManifest("metadata/3ccdc97b-a744-4930-98c7-4abc66c26625-m0.avro")),
          std::make_shared<Manifest>(ReadManifest("metadata/41f34bc8-eedf-4573-96b0-10c04e7c84c4-m0.avro")),
          std::make_shared<Manifest>(ReadManifest("metadata/66ee5e5e-f6c6-47a5-a609-1a439232d1ea-m0.avro"))};
}

std::shared_ptr<TableMetadataV2> DefaultTableMetadata() {
  std::ifstream input("metadata/00000-800cc6aa-5051-47d5-9579-46aafcba1de6.metadata.json");
  return ice_tea::ReadTableMetadataV2(input);
}

tools::Validator DefaultValidator() { return tools::Validator(DefaultRestictions()); }

}  // namespace

TEST(Restrictions, ReadRestrictions) {
  EXPECT_EQ(tools::ReadRestrictions("warehouse/restrictions.json"), DefaultRestictions());
}

TEST(ManifestValidation, OKManifests) { EXPECT_NO_THROW(DefaultValidator().ValidateManifests(DefaultManifests())); }

TEST(ManifestValidation, MaxDataFilesCount) {
  auto restrictions = DefaultRestictions();
  restrictions.max_data_files_count = 0;
  auto validator = tools::Validator(restrictions);
  EXPECT_THROW(validator.ValidateManifests(DefaultManifests()), std::runtime_error);
}

TEST(ManifestValidation, MaxDeleteFilesCount) {
  auto restrictions = DefaultRestictions();
  restrictions.max_delete_files_count = 0;
  auto validator = tools::Validator(restrictions);
  EXPECT_THROW(validator.ValidateManifests(DefaultManifests()), std::runtime_error);
}

TEST(ManifestValidation, MaxPositionDeletesCount) {
  auto restrictions = DefaultRestictions();
  restrictions.max_position_deletes_count = 0;
  auto validator = tools::Validator(restrictions);
  EXPECT_THROW(validator.ValidateManifests(DefaultManifests()), std::runtime_error);
}

TEST(ManifestValidation, MaxEqualityDeletesCount) {
  auto restrictions = DefaultRestictions();
  restrictions.max_equality_deletes_count = 0;
  tools::Validator validator(restrictions);
  auto manifests = DefaultManifests();
  manifests.back()->entries.back().data_file.content = ContentFile::FileContent::kEqualityDeletes;
  EXPECT_THROW(validator.ValidateManifests(manifests), std::runtime_error);
}

TEST(ManifestValidation, MaxRowGroupsPerFile) {
  auto restrictions = DefaultRestictions();
  restrictions.max_row_groups_per_file = 0;
  auto validator = tools::Validator(restrictions);
  EXPECT_THROW(validator.ValidateManifests(DefaultManifests()), std::runtime_error);
}

TEST(ManifestValidation, RowGroupsSizes) {
  const size_t split_offsets_size = 11;

  auto manifests = DefaultManifests();
  manifests.back()->entries.back().data_file.split_offsets.assign(split_offsets_size, 0);
  EXPECT_THROW(DefaultValidator().ValidateManifests(manifests), std::runtime_error);
  for (size_t i = 0; i < split_offsets_size; i++) {
    manifests.back()->entries.back().data_file.split_offsets[i] = 1000000 * i;
  }
  manifests.back()->entries.back().data_file.file_size_in_bytes = split_offsets_size * 1000000;
  EXPECT_THROW(DefaultValidator().ValidateManifests(manifests), std::runtime_error);
}

TEST(ManifestValidation, FileFormat) {
  auto manifests = DefaultManifests();
  manifests.back()->entries.back().data_file.file_format = "AVRO";
  EXPECT_THROW(DefaultValidator().ValidateManifests(manifests), std::runtime_error);
}

TEST(TableMetadataValidation, OKTableMetadata) {
  EXPECT_NO_THROW(DefaultValidator().ValidateTableMetadata(DefaultTableMetadata()));
}

TEST(TableMetadataValidation, Properties) {
  auto table_metadata = DefaultTableMetadata();
  table_metadata->properties.erase("write.parquet.compression-codec");
  EXPECT_THROW(DefaultValidator().ValidateTableMetadata(table_metadata), std::runtime_error);
  table_metadata->properties["write.parquet.compression-codec"] = "wrong argument";
  EXPECT_THROW(DefaultValidator().ValidateTableMetadata(table_metadata), std::runtime_error);
  table_metadata->properties["write.parquet.compression-codec"] = "zstd";
  table_metadata->properties["created-at"] = "wrong argument";
  EXPECT_NO_THROW(DefaultValidator().ValidateTableMetadata(table_metadata));
}

TEST(TableMetadataValidation, Partitioned) {
  auto table_metadata = DefaultTableMetadata();
  table_metadata->partition_specs.clear();
  auto validator = DefaultValidator();
  EXPECT_THROW(DefaultValidator().ValidateTableMetadata(table_metadata), std::runtime_error);
  validator.GetRestrictions().partitioned.clear();
  EXPECT_NO_THROW(validator.ValidateTableMetadata(table_metadata));
}

TEST(TableMetadataValidation, Sorted) {
  auto table_metadata = DefaultTableMetadata();
  table_metadata->sort_orders.clear();
  auto validator = DefaultValidator();
  EXPECT_THROW(validator.ValidateTableMetadata(table_metadata), std::runtime_error);
  validator.GetRestrictions().sorted.clear();
  EXPECT_NO_THROW(validator.ValidateTableMetadata(table_metadata));
}

TEST(TableMetadataValidation, ForbiddenTypes) {
  auto restrictions = DefaultRestictions();
  restrictions.forbidden_types.insert(iceberg::TypeID::kLong);
  EXPECT_THROW(tools::Validator(restrictions).ValidateTableMetadata(DefaultTableMetadata()), std::runtime_error);
}

TEST(TableMetadataValidation, RequiredTags) {
  auto restrictions = DefaultRestictions();
  restrictions.required_tags.push_back("chill");
  EXPECT_THROW(tools::Validator(restrictions).ValidateTableMetadata(DefaultTableMetadata()), std::runtime_error);
}

#if 0
    TEST_F(ValidationTest, MaxFileSize) {
        validator.GetRestrictions().max_array_dimensionality = 0;
        std::shared_ptr<Schema> badschema = std::make_shared<Schema>(0, {});
        // types::NestedField{"chill", 0, false, std::make_shared<const types::ListType>(0, true, nullptr)
        std::swap(table_metadata->schemas[0], badschema);
        EXPECT_THROW(validator.ValidateTableMetadata(table_metadata), std::runtime_error);
        validator.GetRestrictions().max_array_dimensionality = 1;
        std::swap(table_metadata->schemas[0], badschema);
    }
#endif
}  // namespace iceberg
