#include "iceberg/streams/iceberg/data_scan.h"

#include "gtest/gtest.h"
#include "iceberg/common/batch.h"
#include "iceberg/streams/iceberg/data_entries_meta_stream.h"
#include "iceberg/streams/iceberg/iceberg_batch.h"
#include "iceberg/streams/iceberg/plan.h"
#include "iceberg/streams/ut/batch_maker.h"
#include "iceberg/streams/ut/mock_stream.h"

namespace iceberg {
namespace {

class DataScanTest : public ::testing::Test {
 public:
  void SetUp() {
    {
      auto col1 = MakeInt16ArrowColumn({0, 1, 2, 3, 4});
      auto batch1 = MakeBatch({col1}, {"col1"});
      PartitionLayerFilePosition state(PartitionLayerFile(PartitionLayer(0, 0), "p1"), 0);

      auto iceberg_batch1 = IcebergBatch(BatchWithSelectionVector(batch1, SelectionVector<int32_t>(5)), state);
      batches_.emplace_back(std::move(iceberg_batch1));
    }
    {
      auto col1 = MakeInt16ArrowColumn({5, 6, 7, 8, 9});
      auto batch2 = MakeBatch({col1}, {"col1"});
      PartitionLayerFilePosition state(PartitionLayerFile(PartitionLayer(0, 0), "p2"), 0);

      auto iceberg_batch1 = IcebergBatch(BatchWithSelectionVector(batch2, SelectionVector<int32_t>(5)), state);
      batches_.emplace_back(std::move(iceberg_batch1));
    }
  }

  class MockStreamBuilder : public DataScanner::IIcebergStreamBuilder {
   public:
    explicit MockStreamBuilder(std::map<std::string, IcebergBatch> path_to_batch)
        : path_to_batch_(std::move(path_to_batch)) {}

    IcebergStreamPtr Build(const AnnotatedDataPath& path) override {
      std::vector<IcebergBatch> input_batches;
      if (path_to_batch_.contains(path.GetPath())) {
        input_batches.emplace_back(path_to_batch_.at(path.GetPath()));
      }
      return std::make_shared<MockStream<IcebergBatch>>(std::move(input_batches));
    }

   private:
    std::map<std::string, IcebergBatch> path_to_batch_;
  };

  std::vector<IcebergBatch> batches_;
};

TEST_F(DataScanTest, Trivial) {
  auto stream_builder = std::make_shared<MockStreamBuilder>(
      std::map<std::string, IcebergBatch>{{"p1", batches_.at(0)}, {"p2", batches_.at(1)}});

  std::vector<AnnotatedDataPath> annotated_data_paths_;
  PartitionLayerFilePosition state1(PartitionLayerFile(PartitionLayer(0, 0), "p1"), 0);
  PartitionLayerFilePosition state2(PartitionLayerFile(PartitionLayer(0, 0), "p2"), 0);

  annotated_data_paths_.emplace_back(std::move(state1), std::vector<AnnotatedDataPath::Segment>{});
  annotated_data_paths_.emplace_back(std::move(state2), std::vector<AnnotatedDataPath::Segment>{});

  AnnotatedDataPathStreamPtr entries_stream =
      std::make_shared<MockStream<AnnotatedDataPath>>(std::move(annotated_data_paths_));

  DataScanner data_scanner(entries_stream, stream_builder);

  {
    auto batch1 = data_scanner.ReadNext();
    ASSERT_TRUE(batch1 != nullptr);
    batch1->GetRecordBatch()->GetColumnByName("col1")->Equals(batches_.at(0).GetRecordBatch()->GetColumnByName("col1"));
  }
  {
    auto batch2 = data_scanner.ReadNext();
    ASSERT_TRUE(batch2 != nullptr);
    batch2->GetRecordBatch()->GetColumnByName("col1")->Equals(batches_.at(1).GetRecordBatch()->GetColumnByName("col1"));
  }
  {
    auto batch3 = data_scanner.ReadNext();
    ASSERT_TRUE(batch3 == nullptr);
  }
}

}  // namespace
}  // namespace iceberg
