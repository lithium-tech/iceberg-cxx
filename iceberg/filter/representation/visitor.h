#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/filter/representation/node.h"

namespace iceberg::filter {

template <typename Visitor>
typename Visitor::ResultType Accept(NodePtr node, Visitor& visitor) {
  switch (node->node_type) {
    case NodeType::kConst:
      return visitor.TypedVisit(std::static_pointer_cast<ConstNode>(node));
    case NodeType::kFunction:
      return visitor.TypedVisit(std::static_pointer_cast<FunctionNode>(node));
    case NodeType::kIf:
      return visitor.TypedVisit(std::static_pointer_cast<IfNode>(node));
    case NodeType::kVariable:
      return visitor.TypedVisit(std::static_pointer_cast<VariableNode>(node));
    case NodeType::kScalarOverArrayFunction:
      return visitor.TypedVisit(std::static_pointer_cast<ScalarOverArrayFunctionNode>(node));
    case NodeType::kLogical:
      return visitor.TypedVisit(std::static_pointer_cast<LogicalNode>(node));
  }
  throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": internal error"));
}

class ChildrenGetter {
 public:
  using ResultType = std::vector<NodePtr>;

  ResultType Visit(NodePtr node) { return Accept(node, *this); }
  ResultType TypedVisit(std::shared_ptr<FunctionNode> node) { return node->arguments; }
  ResultType TypedVisit(std::shared_ptr<ConstNode> node) { return {}; }
  ResultType TypedVisit(std::shared_ptr<VariableNode> node) { return {}; }
  ResultType TypedVisit(std::shared_ptr<ScalarOverArrayFunctionNode> node) { return {node->scalar}; }
  ResultType TypedVisit(std::shared_ptr<IfNode> node) { return {node->condition, node->then_node, node->else_node}; }
  ResultType TypedVisit(std::shared_ptr<LogicalNode> node) { return node->arguments; }

  static std::vector<NodePtr> GetChildren(NodePtr node) {
    ChildrenGetter getter;
    return getter.Visit(node);
  }
};

class TypeGetter {
 public:
  using ResultType = ValueType;

  ResultType Visit(NodePtr node) { return Accept(node, *this); }
  ResultType TypedVisit(std::shared_ptr<FunctionNode> node) { return node->function_signature.return_type; }
  ResultType TypedVisit(std::shared_ptr<ConstNode> node) { return node->value.GetValueType(); }
  ResultType TypedVisit(std::shared_ptr<VariableNode> node) { return node->value_type; }
  ResultType TypedVisit(std::shared_ptr<ScalarOverArrayFunctionNode> node) { return ValueType::kBool; }
  ResultType TypedVisit(std::shared_ptr<IfNode> node) { return Visit(node->then_node); }
  ResultType TypedVisit(std::shared_ptr<LogicalNode> node) { return ValueType::kBool; }
};

template <typename AggregatingVisitor>
class SubtreeVisitor {
 public:
  using AggregatedResultType = typename AggregatingVisitor::AggregatedResultType;

  void Visit(NodePtr node) {
    auto children = ChildrenGetter::GetChildren(node);

    for (const auto& child : children) {
      Visit(child);
    }

    visitor.Visit(node);
  }

  template <typename T>
  void TypedVisit(T node) {}

  AggregatedResultType GetResult() { return std::move(visitor).GetResult(); }

 private:
  AggregatingVisitor visitor;
};

}  // namespace iceberg::filter
