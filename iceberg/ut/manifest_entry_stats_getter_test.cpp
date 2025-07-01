#include "iceberg/manifest_entry_stats_getter.h"

#include <iceberg/nested_field.h>
#include <iceberg/schema.h>
#include <iceberg/type.h>

#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "iceberg/filter/representation/value.h"
#include "iceberg/filter/stats_filter/stats.h"
#include "iceberg/manifest_entry.h"

namespace iceberg::filter {
namespace {

using ValueType = iceberg::filter::ValueType;

class ManifestEntryStatsGetterTest : public ::testing::Test {};

template <typename T>
std::vector<uint8_t> ToBytes(const T& value) {
  std::vector<uint8_t> result(sizeof(T));
  std::memcpy(result.data(), &value, sizeof(T));
  return result;
}

template <>
std::vector<uint8_t> ToBytes(const std::string& str) {
  std::vector<uint8_t> result(str.size());
  std::memcpy(result.data(), str.data(), str.size());
  return result;
}

iceberg::types::NestedField MakePrimitiveField(iceberg::TypeID id) {
  return iceberg::types::NestedField{
      .name = "name", .field_id = 1, .is_required = true, .type = std::make_shared<iceberg::types::PrimitiveType>(id)};
}

TEST_F(ManifestEntryStatsGetterTest, Boolean) {
  iceberg::ManifestEntry entry;
  entry.data_file.lower_bounds[1] = ToBytes(false);
  entry.data_file.upper_bounds[1] = ToBytes(true);

  std::vector<iceberg::types::NestedField> fields{MakePrimitiveField(iceberg::TypeID::kBoolean)};
  auto schema = std::make_shared<const iceberg::Schema>(1, std::move(fields));

  ManifestEntryStatsGetter getter(entry, schema);
  auto stats = getter.GetStats("name", ValueType::kBool);
  ASSERT_TRUE(stats.has_value());

  EXPECT_EQ(stats->min_max.Get<ValueType::kBool>().min, false);
  EXPECT_EQ(stats->min_max.Get<ValueType::kBool>().max, true);
}

TEST_F(ManifestEntryStatsGetterTest, Int2) {
  iceberg::ManifestEntry entry;
  entry.data_file.lower_bounds[1] = ToBytes(42);
  entry.data_file.upper_bounds[1] = ToBytes(2101);

  std::vector<iceberg::types::NestedField> fields{MakePrimitiveField(iceberg::TypeID::kInt)};
  auto schema = std::make_shared<const iceberg::Schema>(1, std::move(fields));

  ManifestEntryStatsGetter getter(entry, schema);
  auto stats = getter.GetStats("name", ValueType::kInt2);
  ASSERT_TRUE(stats.has_value());

  EXPECT_EQ(stats->min_max.Get<ValueType::kInt2>().min, 42);
  EXPECT_EQ(stats->min_max.Get<ValueType::kInt2>().max, 2101);
}

TEST_F(ManifestEntryStatsGetterTest, Int4) {
  iceberg::ManifestEntry entry;
  entry.data_file.lower_bounds[1] = ToBytes(42);
  entry.data_file.upper_bounds[1] = ToBytes(2101);

  std::vector<iceberg::types::NestedField> fields{MakePrimitiveField(iceberg::TypeID::kInt)};
  auto schema = std::make_shared<const iceberg::Schema>(1, std::move(fields));

  ManifestEntryStatsGetter getter(entry, schema);
  auto stats = getter.GetStats("name", ValueType::kInt4);
  ASSERT_TRUE(stats.has_value());

  EXPECT_EQ(stats->min_max.Get<ValueType::kInt4>().min, 42);
  EXPECT_EQ(stats->min_max.Get<ValueType::kInt4>().max, 2101);
}

TEST_F(ManifestEntryStatsGetterTest, Int8) {
  iceberg::ManifestEntry entry;
  entry.data_file.lower_bounds[1] = ToBytes(int64_t(42));
  entry.data_file.upper_bounds[1] = ToBytes(int64_t(2101));

  std::vector<iceberg::types::NestedField> fields{MakePrimitiveField(iceberg::TypeID::kLong)};
  auto schema = std::make_shared<const iceberg::Schema>(1, std::move(fields));

  ManifestEntryStatsGetter getter(entry, schema);
  auto stats = getter.GetStats("name", ValueType::kInt8);
  ASSERT_TRUE(stats.has_value());

  EXPECT_EQ(stats->min_max.Get<ValueType::kInt8>().min, 42);
  EXPECT_EQ(stats->min_max.Get<ValueType::kInt8>().max, 2101);
}

TEST_F(ManifestEntryStatsGetterTest, Float4) {
  iceberg::ManifestEntry entry;
  entry.data_file.lower_bounds[1] = ToBytes(float(42));
  entry.data_file.upper_bounds[1] = ToBytes(float(2101));

  std::vector<iceberg::types::NestedField> fields{MakePrimitiveField(iceberg::TypeID::kFloat)};
  auto schema = std::make_shared<const iceberg::Schema>(1, std::move(fields));

  ManifestEntryStatsGetter getter(entry, schema);
  auto stats = getter.GetStats("name", ValueType::kFloat4);
  ASSERT_FALSE(stats.has_value());
}

TEST_F(ManifestEntryStatsGetterTest, Float8) {
  iceberg::ManifestEntry entry;
  entry.data_file.lower_bounds[1] = ToBytes(double(42));
  entry.data_file.upper_bounds[1] = ToBytes(double(2101));

  std::vector<iceberg::types::NestedField> fields{MakePrimitiveField(iceberg::TypeID::kDouble)};
  auto schema = std::make_shared<const iceberg::Schema>(1, std::move(fields));

  ManifestEntryStatsGetter getter(entry, schema);
  auto stats = getter.GetStats("name", ValueType::kFloat8);
  ASSERT_FALSE(stats.has_value());
}

TEST_F(ManifestEntryStatsGetterTest, String) {
  iceberg::ManifestEntry entry;
  entry.data_file.lower_bounds[1] = ToBytes(std::string("a"));
  entry.data_file.upper_bounds[1] = ToBytes(std::string("bb"));

  std::vector<iceberg::types::NestedField> fields{MakePrimitiveField(iceberg::TypeID::kString)};
  auto schema = std::make_shared<const iceberg::Schema>(1, std::move(fields));

  ManifestEntryStatsGetter getter(entry, schema);
  auto stats = getter.GetStats("name", ValueType::kString);
  ASSERT_TRUE(stats.has_value());

  EXPECT_EQ(stats->min_max.Get<ValueType::kString>().min, std::string("a"));
  EXPECT_EQ(stats->min_max.Get<ValueType::kString>().max, std::string("bb"));
}

TEST_F(ManifestEntryStatsGetterTest, Time) {
  iceberg::ManifestEntry entry;
  entry.data_file.lower_bounds[1] = ToBytes(int64_t(42));
  entry.data_file.upper_bounds[1] = ToBytes(int64_t(2101));

  std::vector<iceberg::types::NestedField> fields{MakePrimitiveField(iceberg::TypeID::kTime)};
  auto schema = std::make_shared<const iceberg::Schema>(1, std::move(fields));

  ManifestEntryStatsGetter getter(entry, schema);
  auto stats = getter.GetStats("name", ValueType::kTime);
  ASSERT_TRUE(stats.has_value());

  EXPECT_EQ(stats->min_max.Get<ValueType::kTime>().min, 42);
  EXPECT_EQ(stats->min_max.Get<ValueType::kTime>().max, 2101);
}

TEST_F(ManifestEntryStatsGetterTest, Timestamp) {
  iceberg::ManifestEntry entry;
  entry.data_file.lower_bounds[1] = ToBytes(int64_t(42));
  entry.data_file.upper_bounds[1] = ToBytes(int64_t(2101));

  std::vector<iceberg::types::NestedField> fields{MakePrimitiveField(iceberg::TypeID::kTimestamp)};
  auto schema = std::make_shared<const iceberg::Schema>(1, std::move(fields));

  ManifestEntryStatsGetter getter(entry, schema);
  auto stats = getter.GetStats("name", ValueType::kTimestamp);
  ASSERT_TRUE(stats.has_value());

  EXPECT_EQ(stats->min_max.Get<ValueType::kTimestamp>().min, 42);
  EXPECT_EQ(stats->min_max.Get<ValueType::kTimestamp>().max, 2101);
}

TEST_F(ManifestEntryStatsGetterTest, Timestamptz) {
  iceberg::ManifestEntry entry;
  entry.data_file.lower_bounds[1] = ToBytes(int64_t(42));
  entry.data_file.upper_bounds[1] = ToBytes(int64_t(2101));

  std::vector<iceberg::types::NestedField> fields{MakePrimitiveField(iceberg::TypeID::kTimestamptz)};
  auto schema = std::make_shared<const iceberg::Schema>(1, std::move(fields));

  ManifestEntryStatsGetter getter(entry, schema);
  auto stats = getter.GetStats("name", ValueType::kTimestamptz);
  ASSERT_TRUE(stats.has_value());

  EXPECT_EQ(stats->min_max.Get<ValueType::kTimestamptz>().min, 42);
  EXPECT_EQ(stats->min_max.Get<ValueType::kTimestamptz>().max, 2101);
}

TEST_F(ManifestEntryStatsGetterTest, Date) {
  iceberg::ManifestEntry entry;
  entry.data_file.lower_bounds[1] = ToBytes(42);
  entry.data_file.upper_bounds[1] = ToBytes(2101);

  std::vector<iceberg::types::NestedField> fields{MakePrimitiveField(iceberg::TypeID::kDate)};
  auto schema = std::make_shared<const iceberg::Schema>(1, std::move(fields));

  ManifestEntryStatsGetter getter(entry, schema);
  auto stats = getter.GetStats("name", ValueType::kDate);
  ASSERT_TRUE(stats.has_value());

  EXPECT_EQ(stats->min_max.Get<ValueType::kDate>().min, 42);
  EXPECT_EQ(stats->min_max.Get<ValueType::kDate>().max, 2101);
}

TEST_F(ManifestEntryStatsGetterTest, TypesMismatch) {
  iceberg::ManifestEntry entry;
  entry.data_file.lower_bounds[1] = ToBytes(false);
  entry.data_file.upper_bounds[1] = ToBytes(true);

  std::vector<iceberg::types::NestedField> fields{MakePrimitiveField(iceberg::TypeID::kBoolean)};
  auto schema = std::make_shared<const iceberg::Schema>(1, std::move(fields));

  ManifestEntryStatsGetter getter(entry, schema);
  auto stats = getter.GetStats("name", ValueType::kInt4);
  ASSERT_FALSE(stats.has_value());
}

TEST_F(ManifestEntryStatsGetterTest, NameMismatch) {
  iceberg::ManifestEntry entry;
  entry.data_file.lower_bounds[1] = ToBytes(false);
  entry.data_file.upper_bounds[1] = ToBytes(true);

  std::vector<iceberg::types::NestedField> fields{MakePrimitiveField(iceberg::TypeID::kBoolean)};
  auto schema = std::make_shared<const iceberg::Schema>(1, std::move(fields));

  ManifestEntryStatsGetter getter(entry, schema);
  auto stats = getter.GetStats("wrongname", ValueType::kBool);
  ASSERT_FALSE(stats.has_value());
}

TEST_F(ManifestEntryStatsGetterTest, MissingStats) {
  iceberg::ManifestEntry entry;

  std::vector<iceberg::types::NestedField> fields{MakePrimitiveField(iceberg::TypeID::kBoolean)};
  auto schema = std::make_shared<const iceberg::Schema>(1, std::move(fields));

  ManifestEntryStatsGetter getter(entry, schema);
  auto stats = getter.GetStats("name", ValueType::kBool);
  ASSERT_FALSE(stats.has_value());
}

}  // namespace
}  // namespace iceberg::filter
