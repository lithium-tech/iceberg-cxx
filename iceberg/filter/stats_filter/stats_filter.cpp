#include "iceberg/filter/stats_filter/stats_filter.h"

#include <algorithm>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "iceberg/filter/representation/function.h"
#include "iceberg/filter/representation/node.h"
#include "iceberg/filter/representation/value.h"
#include "iceberg/filter/representation/visitor.h"
#include "iceberg/filter/stats_filter/registry.h"
#include "iceberg/filter/stats_filter/stats.h"

namespace iceberg::filter {

namespace {

struct BoolStatsAggregator {
 public:
  explicit BoolStatsAggregator(bool use_or)
      : use_or_(use_or), current_result(use_or ? kBoolStatsFalse : kBoolStatsTrue) {}

  bool Add(MinMaxStats<ValueType::kBool> new_value) {
    if (use_or_) {
      current_result = current_result || new_value;
      return current_result == kBoolStatsTrue;
    } else {
      current_result = current_result && new_value;
      return current_result == kBoolStatsFalse;
    }
  }

  GenericStats Result() const { return GenericStats(GenericMinMaxStats::Make<ValueType::kBool>(current_result)); }

 private:
  bool use_or_;
  MinMaxStats<ValueType::kBool> current_result;
};

// clang-format off
template <typename T>
concept Comparable = requires(T x) { x < x; }; // NOLINT readability/braces
// clang-format on

class StatsFilterVisitor {
 public:
  using ResultType = StatsValue;

  explicit StatsFilterVisitor(const IStatsGetter& stats_getter, const Registry& registry,
                              std::shared_ptr<StatsFilter::ILogger> logger)
      : stats_getter_(stats_getter), registry_(registry), logger_(logger) {}

  ResultType Visit(NodePtr node) { return Accept(node, *this); }

  ResultType TypedVisit(std::shared_ptr<FunctionNode> node) {
    std::vector<GenericStats> evaluated_arguments;
    evaluated_arguments.reserve(node->arguments.size());
    for (const auto& arg : node->arguments) {
      ARROW_ASSIGN_OR_RAISE(auto maybe_stats, Visit(arg));
      evaluated_arguments.emplace_back(std::move(maybe_stats));
    }

    return registry_.Evaluate(node->function_signature.function_id, std::move(evaluated_arguments));
  }

  ResultType TypedVisit(std::shared_ptr<ConstNode> node) {
    const auto& value = node->value;
    if (value.IsNull()) {
      return arrow::Status::ExecutionError("ConstNode with null is not supported");
    }

    auto res = std::visit(
        [&]<ValueType value_type>(Tag<value_type>) -> GenericMinMaxStats {
          return GenericMinMaxStats::Make<value_type>(OneValueToStats<value_type>(value.GetValue<value_type>()));
        },
        DispatchTag(value.GetValueType()));

    return GenericStats(std::move(res), CountStats{.contains_null = false, .contains_non_null = true});
  }

  ResultType TypedVisit(std::shared_ptr<VariableNode> node) {
    auto maybe_stats = stats_getter_.GetStats(node->column_name, node->value_type);
    if (maybe_stats.has_value()) {
      return maybe_stats.value();
    } else {
      return arrow::Status::ExecutionError("No stats for ", node->column_name, " with type ",
                                           static_cast<int>(node->value_type), " found");
    }
  }

  ResultType TypedVisit(std::shared_ptr<ScalarOverArrayFunctionNode> node) {
    ARROW_ASSIGN_OR_RAISE(const auto lhs_stats, Visit(node->scalar));

    const auto& arr = node->array;

    BoolStatsAggregator result_aggregator(node->use_or);

    return std::visit(
        [&]<ValueType value_type>(Tag<value_type>) -> ResultType {
          const auto& typed_arr = arr.GetValue<value_type>();
          for (const auto& elem : typed_arr) {
            MinMaxStats<ValueType::kBool> arg = kBoolStatsUnknown;
            if (elem.has_value()) {
              GenericStats rhs_stats(GenericMinMaxStats::Make<value_type>(elem.value()),
                                     CountStats{.contains_null = false, .contains_non_null = true});
              ARROW_ASSIGN_OR_RAISE(auto stats, registry_.Evaluate(node->function_signature.function_id,
                                                                   {lhs_stats, std::move(rhs_stats)}));
              if (stats.min_max.GetValueType() != ValueType::kBool) {
                return arrow::Status::ExecutionError(__PRETTY_FUNCTION__, ": expected bool stats");
              }
              arg = stats.min_max.Get<ValueType::kBool>();
            }
            if (result_aggregator.Add(arg)) {
              return result_aggregator.Result();
            }
          }
          return result_aggregator.Result();
        },
        DispatchTag(arr.GetValueType()));
  }

  ResultType TypedVisit(std::shared_ptr<IfNode> node) {
    ARROW_ASSIGN_OR_RAISE(auto evaluated_cond, Visit(node->condition));
    if (evaluated_cond.min_max.GetValueType() != ValueType::kBool) {
      return arrow::Status::ExecutionError(__PRETTY_FUNCTION__, ": expected bool stats for condition result");
    }
    auto stats = evaluated_cond.min_max.Get<ValueType::kBool>();
    if (stats.min) {
      return Visit(node->then_node);
    }
    if (!stats.max) {
      return Visit(node->else_node);
    }
    ARROW_ASSIGN_OR_RAISE(auto then_res, Visit(node->then_node));
    ARROW_ASSIGN_OR_RAISE(auto else_res, Visit(node->else_node));
    if (then_res.min_max.GetValueType() != else_res.min_max.GetValueType()) {
      return arrow::Status::ExecutionError(__PRETTY_FUNCTION__,
                                           ": expected same value types for then_node and else_node");
    }
    return std::visit(
        [&]<ValueType value_type>(Tag<value_type>) -> arrow::Result<GenericStats> {
          if constexpr (!Comparable<PhysicalType<value_type>>) {
            return arrow::Status::ExecutionError(__PRETTY_FUNCTION__, ": comparison is not supported for type ",
                                                 std::to_string(static_cast<int>(value_type)));
          } else {
            auto then_stats = then_res.min_max.Get<value_type>();
            auto else_stats = else_res.min_max.Get<value_type>();
            auto result_stats = MinMaxStats<value_type>{.min = std::min(then_stats.min, else_stats.min),
                                                        .max = std::max(then_stats.max, else_stats.max)};
            return GenericStats(GenericMinMaxStats::Make<value_type>(std::move(result_stats)));
          }
        },
        DispatchTag(then_res.min_max.GetValueType()));
  }

  ResultType TypedVisit(std::shared_ptr<LogicalNode> node) {
    switch (node->operation) {
      case LogicalNode::Operation::kNot: {
        if (node->arguments.size() == 1) {
          ARROW_ASSIGN_OR_RAISE(auto evaluated_arg, Visit(node->arguments[0]));
          if (evaluated_arg.min_max.GetValueType() != ValueType::kBool) {
            return arrow::Status::ExecutionError(__PRETTY_FUNCTION__, ": expected bool stats");
          }
          evaluated_arg.min_max =
              GenericMinMaxStats::Make<ValueType::kBool>(!(evaluated_arg.min_max.Get<ValueType::kBool>()));
          return GenericStats(evaluated_arg);
        }
        return arrow::Status::ExecutionError(__PRETTY_FUNCTION__, ": unexpected argument number for kNot function");
      }

      case LogicalNode::Operation::kAnd:
      case LogicalNode::Operation::kOr: {
        BoolStatsAggregator result_aggregator(node->operation == LogicalNode::Operation::kOr);
        for (const auto& arg : node->arguments) {
          auto maybe_evaluated_arg = Visit(arg);
          if (!maybe_evaluated_arg.ok()) {
            if (logger_) {
              logger_->Log(maybe_evaluated_arg.status().message());
            }
            result_aggregator.Add(MinMaxStats<ValueType::kBool>{.min = false, .max = true});
          } else {
            auto evaluated_arg = maybe_evaluated_arg.ValueUnsafe();
            if (evaluated_arg.min_max.GetValueType() != ValueType::kBool) {
              return arrow::Status::ExecutionError(__PRETTY_FUNCTION__, ": expected bool stats");
            }
            if (result_aggregator.Add(evaluated_arg.min_max.Get<ValueType::kBool>())) {
              return result_aggregator.Result();
            }
          }
        }
        return result_aggregator.Result();
      }
    }
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": internal error"));
  }

 private:
  std::vector<ValueType> GetArgumentsTypes(const std::vector<GenericStats>& args) const {
    std::vector<ValueType> result;
    result.reserve(args.size());
    for (const auto& arg : args) {
      result.emplace_back(arg.min_max.GetValueType());
    }

    return result;
  }

  const IStatsGetter& stats_getter_;
  const Registry& registry_;
  std::shared_ptr<StatsFilter::ILogger> logger_;
};

}  // namespace

StatsFilter::StatsFilter(NodePtr root, Settings settings) : root_(root), settings_(settings) {}

arrow::Result<GenericStats> StatsFilter::Evaluate(const IStatsGetter& stats_getter) const {
  if (!root_) {
    return arrow::Status::ExecutionError("Evaluate expected non-empty expression");
  }

  Registry registry(
      Registry::Settings{.timestamp_to_timestamptz_shift_us = settings_.timestamp_to_timestamptz_shift_us});
  StatsFilterVisitor visitor(stats_getter, registry, logger_);
  return visitor.Visit(root_);
}

MatchedRows StatsFilter::ApplyFilter(const IStatsGetter& stats_getter) const {
  if (!root_) {
    return MatchedRows::kSome;
  }

  auto maybe_result = Evaluate(stats_getter);
  if (!maybe_result.ok()) {
    if (logger_) {
      logger_->Log(maybe_result.status().message());
    }
    return MatchedRows::kSome;
  }
  auto result = maybe_result.ValueUnsafe();
  if (result.min_max.GetValueType() != ValueType::kBool) {
    return MatchedRows::kSome;
  }
  auto result_bool = result.min_max.Get<ValueType::kBool>();
  if (result_bool == kBoolStatsFalse) {
    return MatchedRows::kNone;
  } else if (result_bool == kBoolStatsTrue) {
    return MatchedRows::kAll;
  } else {
    return MatchedRows::kSome;
  }
}

}  // namespace iceberg::filter
