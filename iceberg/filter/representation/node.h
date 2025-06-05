#pragma once

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/filter/representation/function.h"
#include "iceberg/filter/representation/value.h"

namespace iceberg::filter {

enum class NodeType { kVariable, kConst, kFunction, kScalarOverArrayFunction, kIf, kLogical };

struct Node {
  NodeType node_type;

  explicit Node(NodeType node_type) : node_type(node_type) {}

 protected:
  ~Node() = default;
};

using NodePtr = std::shared_ptr<Node>;

struct VariableNode : public Node {
  ValueType value_type;  // maybe unnecessary?
  std::string column_name;

  explicit VariableNode(ValueType value_type, std::string column_name)
      : Node(NodeType::kVariable), value_type(value_type), column_name(std::move(column_name)) {}
};

struct ConstNode : public Node {
  Value value;

  explicit ConstNode(Value value) : Node(NodeType::kConst), value(std::move(value)) {}
};

struct IfNode : public Node {
  NodePtr condition;
  NodePtr then_node;
  NodePtr else_node;

  explicit IfNode(NodePtr condition, NodePtr then_node, NodePtr else_node)
      : Node(NodeType::kIf), condition(condition), then_node(then_node), else_node(else_node) {
    if (!condition) {
      throw std::runtime_error("IfNode: condition must be set");
    }
    if (!then_node) {
      throw std::runtime_error("IfNode: ThenNode must be set");
    }
    if (!else_node) {
      throw std::runtime_error("IfNode: ElseNode must be set");
    }
  }
};

struct LogicalNode : public Node {
  enum class Operation { kAnd, kOr, kNot };

  Operation operation;
  std::vector<NodePtr> arguments;

  explicit LogicalNode(Operation op, std::vector<NodePtr> arguments)
      : Node(NodeType::kLogical), operation(op), arguments(std::move(arguments)) {
    if (std::any_of(this->arguments.begin(), this->arguments.end(), [](const auto& arg) { return arg == nullptr; })) {
      throw std::runtime_error("LogicalNode: arguments must be set");
    }
    if (operation == Operation::kNot && this->arguments.size() != 1) {
      throw std::runtime_error("LogicalNode: Operation::kNot expects one argument");
    }
  }
};

struct FunctionNode : public Node {
  FunctionSignature function_signature;
  std::vector<NodePtr> arguments;

  explicit FunctionNode(FunctionSignature function_signature, std::vector<NodePtr> arguments)
      : Node(NodeType::kFunction), function_signature(std::move(function_signature)), arguments(std::move(arguments)) {
    if (std::any_of(this->arguments.begin(), this->arguments.end(), [](const auto& arg) { return arg == nullptr; })) {
      throw std::runtime_error("FunctionNode: arguments must be set");
    }
    if (this->function_signature.argument_types.size() != this->arguments.size()) {
      throw std::runtime_error("FunctionNode: argument_types.size() != arguments.size()");
    }
  }
};

// (scalar op array[0]) && (scalar op array[1]) && ... && (scalar op array[k])
// (scalar op array[0]) || (scalar op array[1]) || ... || (scalar op array[k])
struct ScalarOverArrayFunctionNode : public Node {
  FunctionSignature function_signature;
  bool use_or;
  NodePtr scalar;
  ArrayHolder array;

  explicit ScalarOverArrayFunctionNode(FunctionSignature function_signature, bool use_or, NodePtr lhs,
                                       ArrayHolder array)
      : Node(NodeType::kScalarOverArrayFunction),
        function_signature(std::move(function_signature)),
        use_or(use_or),
        scalar(lhs),
        array(std::move(array)) {
    if (!scalar) {
      throw std::runtime_error("ScalarOverArrayFunctionNode: scalar must be set");
    }
    if (this->array.Size() == 0) {
      throw std::runtime_error("ScalarOverArrayFunctionNode: array must contain at least one element");
    }
    if (this->function_signature.argument_types.size() != 2) {
      throw std::runtime_error("ScalarOverArrayFunctionNode: function signature must have 2 arguments");
    }
  }
};

}  // namespace iceberg::filter
