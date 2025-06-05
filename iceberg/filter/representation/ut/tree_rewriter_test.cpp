#include "iceberg/filter/representation/tree_rewriter.h"

#include "gtest/gtest.h"
#include "iceberg/filter/representation/function.h"
#include "iceberg/filter/representation/node.h"
#include "iceberg/filter/representation/value.h"

namespace iceberg::filter {

namespace {

TEST(TreeRewriter, ScalarOverArray) {
  auto scalar = std::make_shared<VariableNode>(ValueType::kInt4, "col1");

  FunctionSignature sig{.function_id = FunctionID::kLessThan,
                        .return_type = ValueType::kBool,
                        .argument_types = std::vector<ValueType>{ValueType::kInt4, ValueType::kInt4}};

  Array<ValueType::kInt4> arr{2, 3, 4};
  ArrayHolder array = ArrayHolder::Make<ValueType::kInt4>(arr);

  auto soaf_node = std::make_shared<ScalarOverArrayFunctionNode>(sig, true, scalar, array);

  auto result_node = TreeRewriter::ScalarOverArrayToLogical(soaf_node);
  ASSERT_EQ(result_node->node_type, NodeType::kLogical);

  auto logical_node = std::static_pointer_cast<LogicalNode>(result_node);
  EXPECT_EQ(logical_node->operation, LogicalNode::Operation::kOr);
  ASSERT_EQ(logical_node->arguments.size(), 3);

  for (size_t i = 0; i < arr.size(); ++i) {
    auto arg = logical_node->arguments[i];
    ASSERT_EQ(arg->node_type, NodeType::kFunction);

    auto func_node = std::static_pointer_cast<FunctionNode>(arg);
    EXPECT_EQ(func_node->function_signature.function_id, FunctionID::kLessThan);
    EXPECT_EQ(func_node->function_signature.return_type, ValueType::kBool);
    EXPECT_EQ(func_node->function_signature.argument_types,
              (std::vector<ValueType>{ValueType::kInt4, ValueType::kInt4}));

    ASSERT_EQ(func_node->arguments.size(), 2);
    EXPECT_EQ(func_node->arguments[0].get(), scalar.get());

    auto rhs = func_node->arguments[1];
    ASSERT_EQ(rhs->node_type, NodeType::kConst);

    auto rhs_const = std::static_pointer_cast<ConstNode>(rhs);
    EXPECT_EQ(rhs_const->value.GetValueType(), ValueType::kInt4);
    EXPECT_EQ(rhs_const->value.GetValue<ValueType::kInt4>(), arr[i]);
  }
}

}  // namespace

}  // namespace iceberg::filter
