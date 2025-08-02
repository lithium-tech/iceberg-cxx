#include "gtest/gtest.h"
#include "iceberg/streams/iceberg/schema_name_mapper.h"

namespace iceberg {
namespace {
TEST(SchemaNameMapper, IsArray) {
  const std::string wrong_json = "{ \"field-id\": 1, \"names\": [\"col1\"] }";
  const std::string correct_json =
      "[ { \"field-id\": 1, \"names\": [\"col1\"] }, { \"field-id\": 3, \"names\": [\"col2\"] } ]";
  EXPECT_THROW(SchemaNameMapper(wrong_json).GetRootMapper(), std::runtime_error);
  EXPECT_NO_THROW(SchemaNameMapper(correct_json).GetRootMapper());
}

TEST(SchemaNameMapper, CorrectStructure) {
  const std::string no_names_array = "[ { \"field-id\": 1 }, { \"field-id\": 3, \"names\": [\"col2\"] } ]";
  EXPECT_THROW(SchemaNameMapper(no_names_array).GetRootMapper(), std::runtime_error);
  const std::string no_field_id = "[ { \"names\": [\"col1\"] }, { \"field-id\": 3, \"names\": [\"col2\"] } ]";
  EXPECT_NO_THROW(SchemaNameMapper(no_field_id).GetRootMapper());
}

TEST(SchemaNameMapper, UniqueNames) {
  const std::string non_unique_names_json1 =
      "[ { \"field-id\": 1, \"names\": [\"col1\"] }, { \"field-id\": 3, \"names\": [\"col1\"] } ]";
  const std::string non_unique_names_json2 =
      "[ { \"field-id\": 1, \"names\": [\"col1\", \"col1\"] }, { \"field-id\": 3, \"names\": [\"col2\"] } ]";
  EXPECT_THROW(SchemaNameMapper(non_unique_names_json1).GetRootMapper(), std::runtime_error);
  EXPECT_THROW(SchemaNameMapper(non_unique_names_json2).GetRootMapper(), std::runtime_error);
}

TEST(SchemaNameMapper, GetFieldIdByName) {
  const std::string json = "[ { \"field-id\": 1, \"names\": [\"col1\"] }, { \"field-id\": 3, \"names\": [\"col2\"] } ]";
  const SchemaNameMapper schema_name_mapper(json);
  auto root_node = schema_name_mapper.GetRootMapper();

  EXPECT_EQ(root_node->GetFieldIdByName("col1"), 1);
  EXPECT_EQ(root_node->GetFieldIdByName("col2"), 3);
  EXPECT_EQ(root_node->GetFieldIdByName("col3"), std::nullopt);
}

}  // namespace
}  // namespace iceberg
