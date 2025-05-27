#include "stats/analyzer.h"

#include "arrow/filesystem/localfs.h"
#include "gtest/gtest.h"
#include "iceberg/puffin.h"
#include "iceberg/test_utils/assertions.h"
#include "iceberg/test_utils/optional_vector.h"
#include "iceberg/test_utils/scoped_temp_dir.h"
#include "iceberg/test_utils/write.h"
#include "stats/puffin.h"

namespace stats {

class AnalyzerOneColumnTest : public ::testing::Test {
 protected:
  void SetUp() override {
    settings_.fs = std::make_shared<arrow::fs::LocalFileSystem>();
    WriteData();
  }

  std::string GetFileUrl(const std::string& filename) { return "file://" + (dir_.path() / filename).generic_string(); }

  void WriteData() {
    file_path_ = GetFileUrl("del_f1.parquet");
    auto column = iceberg::MakeInt32Column(
        "f1", 1,
        iceberg::OptionalVector<int32_t>{std::nullopt, 1, 2, 2, 5, 5, 5, 5, 5, 5, 5, 4, 4, 4, 4, 4, 3, 3, 3, 3});
    ASSERT_OK(iceberg::WriteToFile({column}, file_path_));
  }

  Settings settings_;
  std::string file_path_;
  iceberg::ScopedTempDir dir_;
};

TEST_F(AnalyzerOneColumnTest, Nothing) {
  Analyzer analyzer(settings_);
  analyzer.Analyze(file_path_);

  const auto& result = analyzer.Result();

  ASSERT_EQ(result.sketches.size(), 1);
  ASSERT_TRUE(result.sketches.contains("f1"));

  const auto& sketch = result.sketches.at("f1");
  ASSERT_EQ(sketch.type, parquet::Type::INT32);
  EXPECT_EQ(sketch.field_id, 1);

  EXPECT_EQ(sketch.counter, std::nullopt);
  EXPECT_EQ(sketch.frequent_items_sketch, std::nullopt);
  EXPECT_EQ(sketch.frequent_items_sketch_dictionary, std::nullopt);
  EXPECT_EQ(sketch.quantile_sketch, std::nullopt);
  EXPECT_EQ(sketch.quantile_sketch_dictionary, std::nullopt);
}

TEST_F(AnalyzerOneColumnTest, Distinct) {
  settings_.evaluate_distinct = true;
  Analyzer analyzer(settings_);

  analyzer.Analyze(file_path_);

  const auto& result = analyzer.Result();

  ASSERT_EQ(result.sketches.size(), 1);
  ASSERT_TRUE(result.sketches.contains("f1"));

  const auto& sketch = result.sketches.at("f1");
  EXPECT_EQ(sketch.frequent_items_sketch, std::nullopt);
  EXPECT_EQ(sketch.frequent_items_sketch_dictionary, std::nullopt);
  EXPECT_EQ(sketch.quantile_sketch, std::nullopt);
  EXPECT_EQ(sketch.quantile_sketch_dictionary, std::nullopt);

  const auto& counter = sketch.counter;
  ASSERT_NE(sketch.counter, std::nullopt);

  EXPECT_EQ(counter->GetDistinctValuesCount(), 5);
}

TEST_F(AnalyzerOneColumnTest, DistinctDictionary) {
  settings_.evaluate_distinct = true;
  settings_.use_dictionary_optimization = true;
  Analyzer analyzer(settings_);

  analyzer.Analyze(file_path_);

  const auto& result = analyzer.Result();
  ASSERT_TRUE(result.sketches.contains("f1"));
  const auto& sketch = result.sketches.at("f1");
  const auto& counter = sketch.counter;
  ASSERT_NE(sketch.counter, std::nullopt);

  EXPECT_EQ(counter->GetDistinctValuesCount(), 5);
}

TEST_F(AnalyzerOneColumnTest, FrequentItems) {
  settings_.evaluate_frequent_items = true;
  Analyzer analyzer(settings_);

  analyzer.Analyze(file_path_);

  const auto& result = analyzer.Result();

  ASSERT_EQ(result.sketches.size(), 1);
  ASSERT_TRUE(result.sketches.contains("f1"));

  const auto& sketch = result.sketches.at("f1");
  EXPECT_EQ(sketch.counter, std::nullopt);
  EXPECT_EQ(sketch.frequent_items_sketch_dictionary, std::nullopt);
  EXPECT_EQ(sketch.quantile_sketch, std::nullopt);
  EXPECT_EQ(sketch.quantile_sketch_dictionary, std::nullopt);

  const auto& frequent_items_sketch = sketch.frequent_items_sketch;
  ASSERT_NE(frequent_items_sketch, std::nullopt);

  EXPECT_EQ(frequent_items_sketch->Type(), stats::Type::kInt64);
  auto frequent_items = frequent_items_sketch->GetFrequentItems<int64_t>();

  ASSERT_EQ(frequent_items.size(), 5);
  EXPECT_EQ(frequent_items[0].first, 5);
  EXPECT_EQ(frequent_items[0].second, 7);

  EXPECT_EQ(frequent_items[1].first, 4);
  EXPECT_EQ(frequent_items[1].second, 5);

  EXPECT_EQ(frequent_items[2].first, 3);
  EXPECT_EQ(frequent_items[2].second, 4);

  EXPECT_EQ(frequent_items[3].first, 2);
  EXPECT_EQ(frequent_items[3].second, 2);

  EXPECT_EQ(frequent_items[4].first, 1);
  EXPECT_EQ(frequent_items[4].second, 1);
}

TEST_F(AnalyzerOneColumnTest, FrequentItemsDictionary) {
  settings_.evaluate_frequent_items = true;
  settings_.use_dictionary_optimization = true;
  Analyzer analyzer(settings_);

  analyzer.Analyze(file_path_);

  const auto& result = analyzer.Result();
  ASSERT_TRUE(result.sketches.contains("f1"));
  const auto& sketch = result.sketches.at("f1");
  const auto& frequent_items_sketch = sketch.frequent_items_sketch;
  ASSERT_NE(frequent_items_sketch, std::nullopt);

  EXPECT_EQ(frequent_items_sketch->Type(), stats::Type::kInt64);
  auto frequent_items = frequent_items_sketch->GetFrequentItems<int64_t>();

  ASSERT_EQ(frequent_items.size(), 5);
  EXPECT_EQ(frequent_items[0].first, 5);
  EXPECT_EQ(frequent_items[0].second, 7);

  EXPECT_EQ(frequent_items[1].first, 4);
  EXPECT_EQ(frequent_items[1].second, 5);

  EXPECT_EQ(frequent_items[2].first, 3);
  EXPECT_EQ(frequent_items[2].second, 4);

  EXPECT_EQ(frequent_items[3].first, 2);
  EXPECT_EQ(frequent_items[3].second, 2);

  EXPECT_EQ(frequent_items[4].first, 1);
  EXPECT_EQ(frequent_items[4].second, 1);
}

TEST_F(AnalyzerOneColumnTest, Quantiles) {
  settings_.evaluate_quantiles = true;
  Analyzer analyzer(settings_);

  analyzer.Analyze(file_path_);

  const auto& result = analyzer.Result();

  ASSERT_EQ(result.sketches.size(), 1);
  ASSERT_TRUE(result.sketches.contains("f1"));

  const auto& sketch = result.sketches.at("f1");
  EXPECT_EQ(sketch.counter, std::nullopt);
  EXPECT_EQ(sketch.frequent_items_sketch, std::nullopt);
  EXPECT_EQ(sketch.frequent_items_sketch_dictionary, std::nullopt);
  EXPECT_EQ(sketch.quantile_sketch_dictionary, std::nullopt);

  const auto& quantile_sketch = sketch.quantile_sketch;
  ASSERT_NE(quantile_sketch, std::nullopt);

  auto bounds = quantile_sketch->GetHistogramBounds<int64_t>(3);

  ASSERT_EQ(bounds.size(), 3);
  EXPECT_EQ(bounds[0], 1);
  EXPECT_EQ(bounds[1], 4);
  EXPECT_EQ(bounds[2], 5);
}

TEST_F(AnalyzerOneColumnTest, QuantilesDictionary) {
  settings_.evaluate_quantiles = true;
  settings_.use_dictionary_optimization = true;
  Analyzer analyzer(settings_);

  analyzer.Analyze(file_path_);

  const auto& result = analyzer.Result();
  ASSERT_TRUE(result.sketches.contains("f1"));
  const auto& sketch = result.sketches.at("f1");
  const auto& quantile_sketch = sketch.quantile_sketch;
  ASSERT_NE(quantile_sketch, std::nullopt);

  auto bounds = quantile_sketch->GetHistogramBounds<int64_t>(3);

  ASSERT_EQ(bounds.size(), 3);
  EXPECT_EQ(bounds[0], 1);
  EXPECT_EQ(bounds[1], 4);
  EXPECT_EQ(bounds[2], 5);
}

TEST_F(AnalyzerOneColumnTest, SketchesToPuffin) {
  settings_.evaluate_distinct = true;
  Analyzer analyzer(settings_);

  analyzer.Analyze(file_path_);

  const auto& result = analyzer.Result();

  iceberg::PuffinFileBuilder builder;
  builder.SetSnapshotId(1);
  builder.SetSequenceNumber(2);

  SketchesToPuffin(result, builder);
  iceberg::PuffinFile file = std::move(builder).Build();

  const auto& footer = file.GetFooter();
  ASSERT_EQ(footer.GetBlobsCount(), 1);

  {
    const auto& blob_meta = footer.GetBlobMetadata(0);
    EXPECT_EQ(blob_meta.fields, std::vector<int32_t>{1});
    ASSERT_TRUE(blob_meta.properties.contains("ndv"));
    EXPECT_EQ(blob_meta.properties.at("ndv"), "5");

    EXPECT_EQ(blob_meta.type, "apache-datasketches-theta-v1");
  }

  iceberg::Statistics stats = PuffinInfoToStatistics(file, "path", 1);
  EXPECT_EQ(stats.statistics_path, "path");
  EXPECT_EQ(stats.snapshot_id, 1);
  EXPECT_EQ(stats.file_size_in_bytes, file.GetPayload().size());
  EXPECT_EQ(stats.file_footer_size_in_bytes, file.GetFooter().GetPayload().size());

  ASSERT_EQ(stats.blob_metadata.size(), 1);
  const auto& blob = stats.blob_metadata[0];
  EXPECT_EQ(blob.field_ids, std::vector<int32_t>{1});
  EXPECT_EQ(blob.sequence_number, 2);
  EXPECT_EQ(blob.type, "apache-datasketches-theta-v1");
  ASSERT_TRUE(blob.properties.contains("ndv"));
  EXPECT_EQ(blob.properties.at("ndv"), "5");
  EXPECT_EQ(blob.snapshot_id, 1);
}

}  // namespace stats
