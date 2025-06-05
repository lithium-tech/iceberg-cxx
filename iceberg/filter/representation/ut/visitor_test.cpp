#include "iceberg/filter/representation/visitor.h"

#include "gtest/gtest.h"
#include "iceberg/filter/representation/node.h"

namespace iceberg::filter {

namespace {

TEST(VisitorTest, Simple) {
  auto condition = std::make_shared<filter::VariableNode>(ValueType::kBool, "col1");
  auto then_node = std::make_shared<filter::ConstNode>(Value::Make<ValueType::kInt2>(42));
  auto else_node = std::make_shared<filter::ConstNode>(Value::Make<ValueType::kInt4>(2101));

  auto root_node = std::make_shared<filter::IfNode>(condition, then_node, else_node);

  ChildrenGetter children_getter;
  auto children = children_getter.Visit(root_node);

  EXPECT_EQ(children.size(), 3);
}

class CountingVisitor {
 public:
  using AggregatedResultType = int32_t;

  void Visit(NodePtr) { ++total_nodes; }

  AggregatedResultType GetResult() { return total_nodes; }

 private:
  int32_t total_nodes = 0;
};

TEST(VisitorTest, SubtreeVisitor) {
  auto condition = std::make_shared<filter::VariableNode>(ValueType::kBool, "col1");
  auto then_node = std::make_shared<filter::ConstNode>(Value::Make<ValueType::kInt2>(42));
  auto else_node = std::make_shared<filter::ConstNode>(Value::Make<ValueType::kInt4>(2101));

  auto root_node = std::make_shared<filter::IfNode>(condition, then_node, else_node);

  SubtreeVisitor<CountingVisitor> visitor;

  visitor.Visit(root_node);
  auto nodes_count = std::move(visitor).GetResult();

  EXPECT_EQ(nodes_count, 4);
}

}  // namespace

}  // namespace iceberg::filter
