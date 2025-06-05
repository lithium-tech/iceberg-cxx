#include "iceberg/filter/representation/node.h"

#include "gtest/gtest.h"
#include "iceberg/filter/representation/function.h"
#include "iceberg/filter/representation/value.h"

namespace iceberg::filter {

namespace {

TEST(NodeTest, Variable) {
  VariableNode var_node(ValueType::kString, "some_name");

  EXPECT_EQ(var_node.node_type, NodeType::kVariable);
  EXPECT_EQ(var_node.value_type, ValueType::kString);
  EXPECT_EQ(var_node.column_name, "some_name");
}

TEST(NodeTest, Const) {
  Value value = Value::Make<ValueType::kString>("qwe");

  ConstNode const_node(std::move(value));

  EXPECT_EQ(const_node.node_type, NodeType::kConst);
  EXPECT_EQ(const_node.value.GetValueType(), ValueType::kString);
  EXPECT_EQ(const_node.value.GetValue<ValueType::kString>(), "qwe");
}

TEST(NodeTest, If) {
  auto cond = std::make_shared<VariableNode>(ValueType::kBool, "col1");
  auto res_true = std::make_shared<ConstNode>(Value::Make<ValueType::kInt4>(2));
  auto res_false = std::make_shared<ConstNode>(Value::Make<ValueType::kInt4>(10));

  EXPECT_ANY_THROW(IfNode if_node(nullptr, res_true, res_false));
  EXPECT_ANY_THROW(IfNode if_node(cond, nullptr, res_false));
  EXPECT_ANY_THROW(IfNode if_node(cond, res_true, nullptr));

  IfNode if_node(cond, res_true, res_false);

  EXPECT_EQ(if_node.node_type, NodeType::kIf);
  EXPECT_EQ(if_node.condition.get(), cond.get());
  EXPECT_EQ(if_node.then_node.get(), res_true.get());
  EXPECT_EQ(if_node.else_node.get(), res_false.get());
}

TEST(NodeTest, Logical) {
  auto arg1 = std::make_shared<VariableNode>(ValueType::kBool, "col1");
  auto arg2 = std::make_shared<ConstNode>(Value::Make<ValueType::kInt4>(2));
  auto arg3 = std::make_shared<ConstNode>(Value::Make<ValueType::kInt4>(10));

  EXPECT_ANY_THROW(LogicalNode logical_node(LogicalNode::Operation::kNot, std::vector<NodePtr>{arg1, arg2, arg3}));
  EXPECT_ANY_THROW(LogicalNode logical_node(LogicalNode::Operation::kAnd, std::vector<NodePtr>{arg1, nullptr, arg3}));

  LogicalNode logical_node(LogicalNode::Operation::kAnd, std::vector<NodePtr>{arg1, arg2, arg3});

  EXPECT_EQ(logical_node.node_type, NodeType::kLogical);
  EXPECT_EQ(logical_node.operation, LogicalNode::Operation::kAnd);
  ASSERT_EQ(logical_node.arguments.size(), 3);
  EXPECT_EQ(logical_node.arguments[0].get(), arg1.get());
  EXPECT_EQ(logical_node.arguments[1].get(), arg2.get());
  EXPECT_EQ(logical_node.arguments[2].get(), arg3.get());
}

TEST(NodeTest, FunctionNode) {
  auto arg1 = std::make_shared<VariableNode>(ValueType::kInt4, "col1");
  auto arg2 = std::make_shared<ConstNode>(Value::Make<ValueType::kInt4>(10));

  FunctionSignature sig{.function_id = FunctionID::kAddWithChecks,
                        .return_type = ValueType::kInt4,
                        .argument_types = std::vector<ValueType>{ValueType::kInt4, ValueType::kInt4}};

  EXPECT_ANY_THROW(FunctionNode function_node(sig, std::vector<NodePtr>{arg1, arg2, arg2}));
  EXPECT_ANY_THROW(FunctionNode function_node(sig, std::vector<NodePtr>{arg1, nullptr}));

  FunctionNode logical_node(sig, std::vector<NodePtr>{arg1, arg2});

  EXPECT_EQ(logical_node.node_type, NodeType::kFunction);
  EXPECT_EQ(logical_node.function_signature.return_type, ValueType::kInt4);
  EXPECT_EQ(logical_node.function_signature.function_id, FunctionID::kAddWithChecks);
  EXPECT_EQ(logical_node.function_signature.argument_types,
            (std::vector<ValueType>{ValueType::kInt4, ValueType::kInt4}));
  ASSERT_EQ(logical_node.arguments.size(), 2);
  EXPECT_EQ(logical_node.arguments[0].get(), arg1.get());
  EXPECT_EQ(logical_node.arguments[1].get(), arg2.get());
}

TEST(NodeTest, ScalarOverArray) {
  auto scalar = std::make_shared<VariableNode>(ValueType::kInt4, "col1");

  FunctionSignature sig{.function_id = FunctionID::kLessThan,
                        .return_type = ValueType::kBool,
                        .argument_types = std::vector<ValueType>{ValueType::kInt4, ValueType::kInt4}};

  ArrayHolder array = ArrayHolder::Make<ValueType::kInt4>(Array<ValueType::kInt4>{2, 3, 4});

  EXPECT_ANY_THROW(ScalarOverArrayFunctionNode(sig, false, nullptr, array));
  EXPECT_ANY_THROW(ScalarOverArrayFunctionNode(
      FunctionSignature{.function_id = FunctionID::kLessThan,
                        .return_type = ValueType::kBool,
                        .argument_types = std::vector<ValueType>{ValueType::kInt4, ValueType::kInt4, ValueType::kInt4}},
      false, scalar, array));

  ScalarOverArrayFunctionNode soaf_node(sig, true, scalar, array);
  EXPECT_EQ(soaf_node.node_type, NodeType::kScalarOverArrayFunction);
  EXPECT_EQ(soaf_node.use_or, true);
  EXPECT_EQ(soaf_node.scalar.get(), scalar.get());
  EXPECT_EQ(soaf_node.array.Size(), array.Size());
  EXPECT_EQ(soaf_node.array.GetValueType(), array.GetValueType());
  std::visit(
      [&]<ValueType value_type>(Tag<value_type>) {
        const auto& lhs = soaf_node.array.GetValue<value_type>();
        const auto& rhs = soaf_node.array.GetValue<value_type>();
        EXPECT_EQ(lhs, rhs);
      },
      DispatchTag(array.GetValueType()));
}

}  // namespace

}  // namespace iceberg::filter
