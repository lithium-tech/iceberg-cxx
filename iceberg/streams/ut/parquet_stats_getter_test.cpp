#include "iceberg/streams/iceberg/parquet_stats_getter.h"

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/util/decimal.h"
#include "gtest/gtest.h"
#include "iceberg/common/fs/url.h"
#include "iceberg/filter/representation/function.h"
#include "iceberg/filter/representation/value.h"
#include "iceberg/filter/stats_filter/stats.h"
#include "iceberg/filter/stats_filter/stats_filter.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/column.h"
#include "iceberg/test_utils/common.h"
#include "iceberg/test_utils/optional_vector.h"
#include "iceberg/test_utils/scoped_temp_dir.h"
#include "iceberg/test_utils/write.h"
#include "parquet/arrow/reader.h"
#include "parquet/metadata.h"

namespace iceberg::filter {
namespace {

using FID = iceberg::filter::FunctionID;

class ParquetStatsGetterTest : public ::testing::Test {
 protected:
  std::string GetFileUrl(const std::string& file_name) {
    return "file://" + (dir_.path() / file_name).generic_string();
  }

  void PrepareData(const std::vector<iceberg::ParquetColumn>& columns) {
    ASSERT_OK(WriteToFile(columns, data_path_));
    PrepareMetaData();

    row_group_meta_ = metadata_->RowGroup(0);
    for (int i = 0; i < metadata_->num_columns(); ++i) {
      std::string name = row_group_meta_->schema()->Column(i)->name();
      name_to_parquet_index_.emplace(name, i);
    }
    stats_getter_.emplace(*row_group_meta_, name_to_parquet_index_);
  }

  void PrepareMetaData() {
    auto components = iceberg::SplitUrl(data_path_);
    std::string path;
    std::shared_ptr<arrow::fs::FileSystem> fs;
    fs = std::make_shared<arrow::fs::LocalFileSystem>();
    path = components.path;
    auto input_file = fs->OpenInputFile(path).ValueOrDie();

    parquet::arrow::FileReaderBuilder reader_builder;
    ASSERT_OK(reader_builder.Open(input_file, parquet::default_reader_properties()));

    reader_builder.memory_pool(arrow::default_memory_pool());
    auto arrow_reader = reader_builder.Build().ValueOrDie();
    metadata_ = arrow_reader->parquet_reader()->metadata();
  }

  iceberg::ScopedTempDir dir_;
  std::string data_path_ = GetFileUrl("data0.parquet");
  std::shared_ptr<parquet::FileMetaData> metadata_;
  std::optional<ParquetStatsGetter> stats_getter_;
  std::unordered_map<std::string, int> name_to_parquet_index_;
  std::shared_ptr<parquet::RowGroupMetaData> row_group_meta_;
};

using MatchedRows = iceberg::filter::MatchedRows;
using ValueType = iceberg::filter::ValueType;
using GenericStats = iceberg::filter::GenericStats;
template <ValueType value_type>
using PhysicalType = iceberg::filter::PhysicalType<value_type>;
template <ValueType value_type>
using Tag = iceberg::filter::Tag<value_type>;

struct Info {
  std::string filter;
  MatchedRows mathed_rows;
};

std::vector<ValueType> GenerateAllValueTypes() {
  return std::vector<ValueType>{ValueType::kBool,    ValueType::kInt2,      ValueType::kInt4,        ValueType::kInt8,
                                ValueType::kFloat4,  ValueType::kFloat8,    ValueType::kNumeric,     ValueType::kString,
                                ValueType::kDate,    ValueType::kTimestamp, ValueType::kTimestamptz, ValueType::kTime,
                                ValueType::kInterval};
}

template <ValueType value_type>
void Check(const std::optional<GenericStats>& maybe_stats, const PhysicalType<value_type>& min,
           const PhysicalType<value_type>& max) {
  ASSERT_TRUE(maybe_stats.has_value());
  const auto& stats = maybe_stats.value().min_max;

  EXPECT_EQ(stats.GetValueType(), value_type);
  const auto& typed_stats = stats.Get<value_type>();

  EXPECT_EQ(typed_stats.min, min);
  EXPECT_EQ(typed_stats.max, max);
}

TEST_F(ParquetStatsGetterTest, Boolean) {
  auto column1 = MakeBoolColumn("col1", 1, OptionalVector<bool>{std::nullopt, false, true});
  PrepareData({column1});

  for (const auto& value_type : GenerateAllValueTypes()) {
    auto result = stats_getter_->GetStats("col1", value_type);
    std::visit(
        [&]<ValueType val_type>(Tag<val_type>) -> void {
          if constexpr (val_type == ValueType::kBool) {
            Check<ValueType::kBool>(result, false, true);
          } else {
            EXPECT_FALSE(result.has_value());
          }
        },
        DispatchTag(value_type));
  }
}

TEST_F(ParquetStatsGetterTest, Int2) {
  auto column1 = MakeInt16Column("col1", 1, OptionalVector<int32_t>{std::nullopt, -2, 14});
  PrepareData({column1});

  for (const auto& value_type : GenerateAllValueTypes()) {
    auto result = stats_getter_->GetStats("col1", value_type);

    std::visit(
        [&]<ValueType val_type>(Tag<val_type>) -> void {
          if constexpr (val_type == ValueType::kInt2 || val_type == ValueType::kInt4) {
            Check<val_type>(result, -2, 14);
          } else {
            EXPECT_FALSE(result.has_value());
          }
        },
        DispatchTag(value_type));
  }
}

TEST_F(ParquetStatsGetterTest, Int4) {
  auto column1 = MakeInt32Column("col1", 1, OptionalVector<int32_t>{std::nullopt, -2, 14});
  PrepareData({column1});

  for (const auto& value_type : GenerateAllValueTypes()) {
    auto result = stats_getter_->GetStats("col1", value_type);

    std::visit(
        [&]<ValueType val_type>(Tag<val_type>) -> void {
          if constexpr (val_type == ValueType::kInt4) {
            Check<val_type>(result, -2, 14);
          } else {
            EXPECT_FALSE(result.has_value()) << "value type " << static_cast<int>(val_type);
          }
        },
        DispatchTag(value_type));
  }
}

TEST_F(ParquetStatsGetterTest, Int8) {
  auto column1 = MakeInt64Column("col1", 1, OptionalVector<int64_t>{std::nullopt, -2, 14});
  PrepareData({column1});

  for (const auto& value_type : GenerateAllValueTypes()) {
    auto result = stats_getter_->GetStats("col1", value_type);

    std::visit(
        [&]<ValueType val_type>(Tag<val_type>) -> void {
          if constexpr (val_type == ValueType::kInt8) {
            Check<val_type>(result, -2, 14);
          } else {
            EXPECT_FALSE(result.has_value()) << "value type " << static_cast<int>(val_type);
          }
        },
        DispatchTag(value_type));
  }
}

TEST_F(ParquetStatsGetterTest, Float4) {
  auto column1 = MakeFloatColumn("col1", 1, OptionalVector<float>{std::nullopt, -2, 14});
  PrepareData({column1});

  for (const auto& value_type : GenerateAllValueTypes()) {
    auto result = stats_getter_->GetStats("col1", value_type);

    std::visit(
        [&]<ValueType val_type>(Tag<val_type>) -> void {
          if constexpr (val_type == ValueType::kFloat4) {
            Check<val_type>(result, -2, 14);
          } else {
            EXPECT_FALSE(result.has_value()) << "value type " << static_cast<int>(val_type);
          }
        },
        DispatchTag(value_type));
  }
}

TEST_F(ParquetStatsGetterTest, Float8) {
  auto column1 = MakeDoubleColumn("col1", 1, OptionalVector<double>{std::nullopt, -2, 14});
  PrepareData({column1});

  for (const auto& value_type : GenerateAllValueTypes()) {
    auto result = stats_getter_->GetStats("col1", value_type);

    std::visit(
        [&]<ValueType val_type>(Tag<val_type>) -> void {
          if constexpr (val_type == ValueType::kFloat8) {
            Check<val_type>(result, -2, 14);
          } else {
            EXPECT_FALSE(result.has_value()) << "value type " << static_cast<int>(val_type);
          }
        },
        DispatchTag(value_type));
  }
}

TEST_F(ParquetStatsGetterTest, String) {
  std::string str1 = "aa";
  std::string str2 = "baca";
  std::string str3 = "www";
  auto column1 = MakeStringColumn("col1", 1, std::vector<std::string*>{nullptr, &str1, &str2, &str3});
  PrepareData({column1});

  for (const auto& value_type : GenerateAllValueTypes()) {
    auto result = stats_getter_->GetStats("col1", value_type);

    std::visit(
        [&]<ValueType val_type>(Tag<val_type>) -> void {
          if constexpr (val_type == ValueType::kString) {
            Check<val_type>(result, str1, str3);
          } else {
            EXPECT_FALSE(result.has_value()) << "value type " << static_cast<int>(val_type);
          }
        },
        DispatchTag(value_type));
  }
}

TEST_F(ParquetStatsGetterTest, Time) {
  auto column1 = MakeTimeColumn("col1", 1, OptionalVector<int64_t>{std::nullopt, -2, 14});
  PrepareData({column1});

  for (const auto& value_type : GenerateAllValueTypes()) {
    auto result = stats_getter_->GetStats("col1", value_type);

    std::visit(
        [&]<ValueType val_type>(Tag<val_type>) -> void {
          if constexpr (val_type == ValueType::kTime) {
            Check<val_type>(result, -2, 14);
          } else {
            EXPECT_FALSE(result.has_value()) << "value type " << static_cast<int>(val_type);
          }
        },
        DispatchTag(value_type));
  }
}

TEST_F(ParquetStatsGetterTest, Timestamp) {
  auto column1 = MakeTimestampColumn("col1", 1, OptionalVector<int64_t>{std::nullopt, -2, 14});
  PrepareData({column1});

  for (const auto& value_type : GenerateAllValueTypes()) {
    auto result = stats_getter_->GetStats("col1", value_type);

    std::visit(
        [&]<ValueType val_type>(Tag<val_type>) -> void {
          if constexpr (val_type == ValueType::kTimestamp) {
            Check<val_type>(result, -2, 14);
          } else {
            EXPECT_FALSE(result.has_value()) << "value type " << static_cast<int>(val_type);
          }
        },
        DispatchTag(value_type));
  }
}

TEST_F(ParquetStatsGetterTest, Timestamptz) {
  auto column1 = MakeTimestamptzColumn("col1", 1, OptionalVector<int64_t>{std::nullopt, -2, 14});
  PrepareData({column1});

  for (const auto& value_type : GenerateAllValueTypes()) {
    auto result = stats_getter_->GetStats("col1", value_type);

    std::visit(
        [&]<ValueType val_type>(Tag<val_type>) -> void {
          if constexpr (val_type == ValueType::kTimestamptz) {
            Check<val_type>(result, -2, 14);
          } else {
            EXPECT_FALSE(result.has_value()) << "value type " << static_cast<int>(val_type);
          }
        },
        DispatchTag(value_type));
  }
}

TEST_F(ParquetStatsGetterTest, Date) {
  auto column1 = MakeDateColumn("col1", 1, OptionalVector<int32_t>{std::nullopt, -2, 14});
  PrepareData({column1});

  for (const auto& value_type : GenerateAllValueTypes()) {
    auto result = stats_getter_->GetStats("col1", value_type);

    std::visit(
        [&]<ValueType val_type>(Tag<val_type>) -> void {
          if constexpr (val_type == ValueType::kDate) {
            Check<val_type>(result, -2, 14);
          } else {
            EXPECT_FALSE(result.has_value()) << "value type " << static_cast<int>(val_type);
          }
        },
        DispatchTag(value_type));
  }
}

std::string Int128ToStringBigEndian(__int128 value, uint32_t bytes_shift = 0) {
  assert(bytes_shift <= 15);
  auto bytes = std::string(reinterpret_cast<const char*>(&value), sizeof(value));
  auto decimal =
      arrow::Decimal128::FromBigEndian(reinterpret_cast<uint8_t*>(bytes.data()), sizeof(__int128)).ValueOrDie();
  std::string result = std::string(reinterpret_cast<const char*>(decimal.native_endian_bytes()), sizeof(__int128));
  if (bytes_shift != 0) {
    for (uint32_t i = 0; i + bytes_shift < sizeof(__int128); ++i) {
      result[i] = result[i + bytes_shift];
    }
    for (uint32_t i = sizeof(__int128) - bytes_shift; i < sizeof(__int128); ++i) {
      result[i] = 0;
    }
  }
  return result;
}

TEST_F(ParquetStatsGetterTest, Numeric) {
  auto column1 = MakeNumericColumn("col1", 1, OptionalVector<int32_t>{std::nullopt, -201, 142}, 7, 2);
  auto column2 = MakeNumericColumn("col1", 1, OptionalVector<int64_t>{std::nullopt, -201, 142}, 7, 2);
  std::string str1 = Int128ToStringBigEndian(-201);
  std::string str2 = Int128ToStringBigEndian(142);
  auto column3 = MakeNumericColumn("col1", 1, std::vector<std::string*>{nullptr, &str1, &str2}, 7, 2);
  auto column4 = MakeNumericColumn("col1", 1, std::vector<std::string*>{nullptr, &str1, &str2}, 7, 2, 16);
  std::string str3 = Int128ToStringBigEndian(-201, 9);
  std::string str4 = Int128ToStringBigEndian(142, 9);
  auto column5 = MakeNumericColumn("col1", 1, std::vector<std::string*>{nullptr, &str3, &str4}, 7, 2, 7);

  for (const auto& column : {column1, column2, column3, column4, column5}) {
    PrepareData({column});

    for (const auto& value_type : GenerateAllValueTypes()) {
      auto result = stats_getter_->GetStats("col1", value_type);

      std::visit(
          [&]<ValueType val_type>(Tag<val_type>) -> void {
            if constexpr (val_type == ValueType::kNumeric) {
              EXPECT_FALSE(result.has_value()) << "value type " << static_cast<int>(val_type);
              // TODO(gmusya): support
            } else {
              EXPECT_FALSE(result.has_value()) << "value type " << static_cast<int>(val_type);
            }
          },
          DispatchTag(value_type));
    }
  }
}

}  // namespace
}  // namespace iceberg::filter
