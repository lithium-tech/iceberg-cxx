#include "iceberg/filter/row_filter/row_filter.h"

#include "arrow/record_batch.h"
#include "arrow/type_fwd.h"
#include "gtest/gtest.h"
#include "iceberg/filter/representation/function.h"
#include "iceberg/filter/representation/node.h"
#include "iceberg/filter/representation/value.h"
#include "iceberg/test_utils/assertions.h"

namespace iceberg::filter {

using ValueType = iceberg::filter::ValueType;

TEST(RowFilter, Test) {
  auto lhs = std::make_shared<iceberg::filter::VariableNode>(ValueType::kInt2, "lhs");
  auto rhs = std::make_shared<iceberg::filter::VariableNode>(ValueType::kInt2, "rhs");

  auto root_node = std::make_shared<iceberg::filter::FunctionNode>(
      iceberg::filter::FunctionSignature{.function_id = iceberg::filter::FunctionID::kLessThan,
                                         .return_type = ValueType::kBool,
                                         .argument_types = std::vector<ValueType>{ValueType::kInt2, ValueType::kInt2}},
      std::vector<iceberg::filter::NodePtr>{lhs, rhs});

  arrow::Int16Builder lhs_builder;
  ASSERT_OK(lhs_builder.AppendValues(std::vector<int16_t>{5, 2, 3, 4, 1}));
  ASSIGN_OR_FAIL(auto lhs_array, lhs_builder.Finish());

  arrow::Int16Builder rhs_builder;
  ASSERT_OK(rhs_builder.AppendValues(std::vector<int16_t>{1, 4, 3, 2, 5}));
  ASSIGN_OR_FAIL(auto rhs_array, rhs_builder.Finish());

  arrow::FieldVector fields{std::make_shared<arrow::Field>("lhs", arrow::int16()),
                            std::make_shared<arrow::Field>("rhs", arrow::int16())};

  auto batch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(fields), 5, {lhs_array, rhs_array});

  RowFilter row_filter(root_node);
  ASSERT_OK(row_filter.BuildFilter(std::make_shared<TrivialArrowFieldResolver>(batch->schema()),
                                   std::make_shared<GandivaFunctionRegistry>(0), batch->schema()));

  ASSIGN_OR_FAIL(auto selection_vector, row_filter.ApplyFilter(batch));
  EXPECT_EQ(selection_vector->GetNumSlots(), 2);
  EXPECT_EQ(selection_vector->GetIndex(0), 1);
  EXPECT_EQ(selection_vector->GetIndex(1), 4);
}

}  // namespace iceberg::filter
