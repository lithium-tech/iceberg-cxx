#pragma once

#include <memory>
#include <set>
#include <string>

#include "iceberg/filter/representation/visitor.h"

namespace iceberg::filter {

class ColumnExtractorVisitor {
 public:
  using ResultType = void;
  using AggregatedResultType = std::set<std::string>;

  void Visit(iceberg::filter::NodePtr node) { return Accept(node, *this); }

  template <typename Anything>
  void TypedVisit(std::shared_ptr<Anything> node) {}

  void TypedVisit(std::shared_ptr<iceberg::filter::VariableNode> node) { result.insert(node->column_name); }

  AggregatedResultType GetResult() const { return result; }

 private:
  AggregatedResultType result;
};
}  // namespace iceberg::filter
