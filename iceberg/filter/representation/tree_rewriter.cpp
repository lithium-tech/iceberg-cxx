#include "iceberg/filter/representation/tree_rewriter.h"

#include <utility>
#include <variant>
#include <vector>

namespace iceberg::filter {

NodePtr TreeRewriter::ScalarOverArrayToLogical(std::shared_ptr<ScalarOverArrayFunctionNode> node) {
  std::vector<NodePtr> function_nodes;
  std::visit(
      [&]<ValueType value_type>(Tag<value_type>) {
        Array<value_type> values = node->array.GetValue<value_type>();
        function_nodes.reserve(values.size());
        for (const auto& maybe_value : values) {
          Value value =
              maybe_value.has_value() ? Value::Make<value_type>(maybe_value.value()) : Value::Make<value_type>();
          NodePtr const_node = std::make_shared<ConstNode>(std::move(value));
          NodePtr op_node =
              std::make_shared<FunctionNode>(node->function_signature, std::vector<NodePtr>{node->scalar, const_node});
          function_nodes.emplace_back(op_node);
        }
      },
      DispatchTag(node->array.GetValueType()));

  return std::make_shared<LogicalNode>(node->use_or ? LogicalNode::Operation::kOr : LogicalNode::Operation::kAnd,
                                       std::move(function_nodes));
}

}  // namespace iceberg::filter
