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
    return SelectionVector<int32_t>(batch->GetRecordBatch()->num_rows());
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
    return static_cast<MockStream*>(this)->ReadNext();
  }

  int GetCounter() const { return cnt_; }

 private:
  int cnt_ = 0;
};

TEST(FilteringStreamTest, BatchesHaveSameSizes) {
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

}  // namespace
}  // namespace iceberg
