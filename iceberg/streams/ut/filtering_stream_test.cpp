#include "iceberg/streams/iceberg/filtering_stream.h"

#include "gtest/gtest.h"
#include "iceberg/streams/ut/batch_maker.h"

namespace iceberg {
namespace {

class MockFilter : public ice_filter::IRowFilter {
 public:
  explicit MockFilter(std::vector<int32_t> involved_field_ids, std::vector<int32_t> batches_for_skipping)
      : ice_filter::IRowFilter(std::move(involved_field_ids)), batches_for_skipping_(std::move(batches_for_skipping)) {}

  SelectionVector<int32_t> ApplyFilter(std::shared_ptr<ArrowBatchWithRowPosition> batch) const override {
    ++current_batch;
    if (ind < batches_for_skipping_.size() && current_batch == batches_for_skipping_[ind]) {
      ++ind;
      return SelectionVector<int32_t>({});
    }
    std::vector<int32_t> indices;
    for (int i = 0; i < batch->GetRecordBatch()->num_rows(); i += 2) {
      indices.push_back(i);
    }
    return SelectionVector<int32_t>(indices);
  }

 private:
  const std::vector<int32_t> batches_for_skipping_;
  mutable int ind = 0;
  mutable int current_batch = -1;
};

class MockStream : public IStream<ArrowBatchWithRowPosition> {
 public:
  explicit MockStream(std::vector<std::shared_ptr<ArrowBatchWithRowPosition>> batches) : batches_(std::move(batches)) {}

  std::shared_ptr<ArrowBatchWithRowPosition> ReadNext() override {
    if (ind_ < batches_.size()) {
      return batches_[ind_++];
    }
    return nullptr;
  }

 private:
  const std::vector<std::shared_ptr<ArrowBatchWithRowPosition>> batches_;
  int ind_ = 0;
};

class CountingStream : public MockStream {
 public:
  explicit CountingStream(std::vector<std::shared_ptr<ArrowBatchWithRowPosition>> batches)
      : MockStream(std::move(batches)) {}

  std::shared_ptr<ArrowBatchWithRowPosition> ReadNext() override {
    ++cnt_;
    return MockStream::ReadNext();
  }

  int GetCounter() const { return cnt_; }

 private:
  int cnt_ = 0;
};

TEST(FilteringStreamTest, Concatenate) {
  auto batch = std::make_shared<ArrowBatchWithRowPosition>(MakeBatch({MakeInt32ArrowColumn({0})}, {"a"}), 0);
  auto batch_with_different_row_position =
      std::make_shared<ArrowBatchWithRowPosition>(MakeBatch({MakeInt32ArrowColumn({1})}, {"b"}), 1);
  auto batch_with_different_number_of_rows =
      std::make_shared<ArrowBatchWithRowPosition>(MakeBatch({MakeInt32ArrowColumn({2, 3})}, {"c"}), 0);
  std::string str = "aba";
  auto correct_batch =
      std::make_shared<ArrowBatchWithRowPosition>(MakeBatch({MakeStringArrowColumn({&str})}, {"d"}), 0);

  EXPECT_THROW(Concatenate(batch, batch_with_different_row_position), std::runtime_error);
  EXPECT_THROW(Concatenate(batch, batch_with_different_number_of_rows), std::runtime_error);

  std::shared_ptr<ArrowBatchWithRowPosition> concatenated_batch;
  EXPECT_NO_THROW(concatenated_batch = Concatenate(batch, correct_batch));
  EXPECT_EQ(concatenated_batch->GetRecordBatch()->num_columns(), 2);
  EXPECT_TRUE(concatenated_batch->GetRecordBatch()->GetColumnByName("d")->Equals(MakeStringArrowColumn({&str})));
  EXPECT_TRUE(concatenated_batch->GetRecordBatch()->GetColumnByName("a")->Equals(MakeInt32ArrowColumn({0})));
}

TEST(FilteringStreamTest, Correctness) {
  std::vector filter_batches = {
      std::make_shared<ArrowBatchWithRowPosition>(MakeBatch({MakeInt32ArrowColumn({0})}, {"a"}), 0),
      std::make_shared<ArrowBatchWithRowPosition>(MakeBatch({MakeInt32ArrowColumn({1, 2})}, {"a"}), 1),
      std::make_shared<ArrowBatchWithRowPosition>(MakeBatch({MakeInt32ArrowColumn({3, 4})}, {"a"}), 3)};

  std::vector data_batches = {
      std::make_shared<ArrowBatchWithRowPosition>(MakeBatch({MakeInt32ArrowColumn({5})}, {"b"}), 0),
      std::make_shared<ArrowBatchWithRowPosition>(MakeBatch({MakeInt32ArrowColumn({6, 7})}, {"b"}), 1),
      std::make_shared<ArrowBatchWithRowPosition>(MakeBatch({MakeInt32ArrowColumn({8, 9})}, {"b"}), 3)};

  auto filter_stream = std::make_shared<CountingStream>(std::move(filter_batches));
  auto data_stream = std::make_shared<CountingStream>(std::move(data_batches));

  auto row_filter = std::make_shared<MockFilter>(std::vector<int32_t>{}, std::vector<int32_t>{0, 2});

  auto result_stream = std::make_shared<FilteringStream>(filter_stream, data_stream, row_filter,
                                                         PartitionLayerFile(PartitionLayer(0, 0), ""));
  auto batch1 = result_stream->ReadNext();
  ASSERT_TRUE(batch1);

  EXPECT_EQ(batch1->GetRecordBatch()->num_columns(), 2);
  EXPECT_TRUE(batch1->GetRecordBatch()->GetColumnByName("a")->Equals(*MakeInt32ArrowColumn({1, 2})));
  EXPECT_TRUE(batch1->GetRecordBatch()->GetColumnByName("b")->Equals(*MakeInt32ArrowColumn({6, 7})));

  EXPECT_EQ(batch1->GetSelectionVector().GetVector(), std::vector<int32_t>{0});

  EXPECT_EQ(filter_stream->GetCounter(), 2);
  EXPECT_EQ(data_stream->GetCounter(), 2);

  auto batch2 = result_stream->ReadNext();
  EXPECT_EQ(batch2, nullptr);

  EXPECT_EQ(filter_stream->GetCounter(), 4);  // 3 for batches + 1 for last nullptr
  EXPECT_EQ(data_stream->GetCounter(), 2);
}

}  // namespace
}  // namespace iceberg
