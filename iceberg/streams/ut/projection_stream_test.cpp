#include "iceberg/streams/arrow/projection_stream.h"

#include <arrow/type.h>

#include "gtest/gtest.h"
#include "iceberg/streams/arrow/batch_with_row_number.h"
#include "iceberg/streams/ut/batch_maker.h"
#include "iceberg/streams/ut/mock_stream.h"

namespace iceberg {
namespace {

TEST(ProjectionStream, Trivial) {
  std::map<std::string, std::string> old_name_to_new_name{{"old1", "new1"}, {"old2", "new2"}};

  auto col1 = MakeInt32ArrowColumn({1, 2, 3});
  auto col2 = MakeInt32ArrowColumn({4, 5, 6});
  auto col3 = MakeInt32ArrowColumn({7, 8, 9});

  auto batch = MakeBatch({col1, col2, col3}, {"old2", "old3", "old1"});

  auto batch_with_row_number = ArrowBatchWithRowPosition(batch, 12);
  auto mock_stream = std::make_shared<MockStream<ArrowBatchWithRowPosition>>(
      std::vector<ArrowBatchWithRowPosition>{batch_with_row_number});

  auto stream = std::make_shared<ProjectionStream>(std::move(old_name_to_new_name), mock_stream);

  auto result = stream->ReadNext();
  ASSERT_NE(result, nullptr);

  EXPECT_EQ(result->row_position, 12);
  ASSERT_NE(result->batch, nullptr);

  EXPECT_EQ(result->batch->num_columns(), 2);

  auto expected_result = MakeBatch({col1, col3}, {"new2", "new1"});

  EXPECT_TRUE(result->batch->GetColumnByName("new1")->Equals(expected_result->GetColumnByName("new1")));
  EXPECT_TRUE(result->batch->GetColumnByName("new2")->Equals(expected_result->GetColumnByName("new2")));

  ASSERT_EQ(stream->ReadNext(), nullptr);
}

}  // namespace
}  // namespace iceberg
