#include "iceberg/filter/row_filter/row_filter.h"

#include <iceberg/common/logger.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "gandiva/filter.h"
#include "gandiva/gandiva_aliases.h"
#include "gandiva/tree_expr_builder.h"
#include "iceberg/filter/representation/function.h"
#include "iceberg/filter/representation/node.h"
#include "iceberg/filter/representation/tree_rewriter.h"
#include "iceberg/filter/representation/value.h"
#include "iceberg/filter/representation/visitor.h"
#include "iceberg/filter/row_filter/arrow_types.h"
#include "iceberg/filter/row_filter/extend_gandiva/register_functions.h"

namespace iceberg::filter {

namespace {
using ValueType = iceberg::filter::ValueType;

using Builder = gandiva::TreeExprBuilder;

void PromoteToInt8(gandiva::NodePtr& gnode) { gnode = Builder::MakeFunction("castBIGINT", {gnode}, arrow::int64()); }

void PromoteToInt4(gandiva::NodePtr& gnode) { gnode = Builder::MakeFunction("castINT", {gnode}, arrow::int32()); }

void PromoteToFloat8(gandiva::NodePtr& gnode) {
  gnode = Builder::MakeFunction("castFLOAT8", {gnode}, arrow::float64());
}

arrow::Result<arrow::Decimal128> StringToDecimal(const std::string& raw_number, int* scale, int* precision) {
  arrow::Decimal128 decimal;
  if (auto status = arrow::Decimal128::FromString(raw_number, &decimal, precision, scale); !status.ok()) {
    return status;
  }
  return decimal;
}

arrow::Result<gandiva::DecimalScalar128> StringToDecimalScalar(const std::string& raw_number) {
  int scale = 0;
  int precision = 0;
  ARROW_ASSIGN_OR_RAISE(auto decimal, StringToDecimal(raw_number, &scale, &precision));
  return gandiva::DecimalScalar128(decimal, (precision ? precision : 1), scale);
}

class RowFilterVisitor {
 public:
  RowFilterVisitor(std::shared_ptr<const IArrowFieldResolver> arrow_field_resolver,
                   std::shared_ptr<const IGandivaFunctionRegistry> function_registry)
      : arrow_field_resolver_(arrow_field_resolver), function_registry_(function_registry) {
    if (!arrow_field_resolver) {
      throw std::runtime_error("RowFilterVisitor: arrow_field_resolver is not set");
    }
    if (!function_registry) {
      throw std::runtime_error("RowFilterVisitor: function_registry is not set");
    }
  }
  using ResultType = arrow::Result<gandiva::NodePtr>;

  ResultType Visit(iceberg::filter::NodePtr node) { return Accept(node, *this); }

  ResultType TypedVisit(std::shared_ptr<iceberg::filter::FunctionNode> node) {
    gandiva::NodeVector evaluated_arguments;
    evaluated_arguments.reserve(node->arguments.size());

    for (const auto& arg : node->arguments) {
      ARROW_ASSIGN_OR_RAISE(auto evaluated_arg, Visit(arg));
      evaluated_arguments.emplace_back(evaluated_arg);
    }

    return function_registry_->CreateFunction(node->function_signature.function_id, evaluated_arguments);
  }

  ResultType TypedVisit(std::shared_ptr<iceberg::filter::ConstNode> node) {
    const auto& value = node->value;
    if (value.IsNull()) {
      return ProcessNull(value.GetValueType());
    }
    switch (value.GetValueType()) {
      case ValueType::kBool:
        return Builder::MakeLiteral(value.GetValue<ValueType::kBool>());
      case ValueType::kInt2:
        return Builder::MakeLiteral(value.GetValue<ValueType::kInt2>());
      case ValueType::kInt4:
        return Builder::MakeLiteral(value.GetValue<ValueType::kInt4>());
      case ValueType::kInt8:
        return Builder::MakeLiteral(value.GetValue<ValueType::kInt8>());
      case ValueType::kFloat4:
        return Builder::MakeLiteral(value.GetValue<ValueType::kFloat4>());
      case ValueType::kFloat8:
        return Builder::MakeLiteral(value.GetValue<ValueType::kFloat8>());
      case ValueType::kString:
        return Builder::MakeStringLiteral(value.GetValue<ValueType::kString>());
      case ValueType::kDate: {
        auto date_node = Builder::MakeLiteral(value.GetValue<ValueType::kDate>());
        return Builder::MakeFunction("castDate", {date_node}, arrow::date32());
      }
      case ValueType::kTimestamp: {
        auto timestamp_node = Builder::MakeLiteral(value.GetValue<ValueType::kTimestamp>());
        return Builder::MakeFunction("castTIMESTAMP", {timestamp_node}, ArrowTimestampType());
      }
      case ValueType::kTimestamptz: {
        auto timestamp_node = Builder::MakeLiteral(value.GetValue<ValueType::kTimestamptz>());
        return Builder::MakeFunction("castTIMESTAMPTZ", {timestamp_node}, ArrowTimestamptzType());
      }
      case ValueType::kNumeric: {
        std::string raw_number = value.GetValue<ValueType::kNumeric>().value;
        ARROW_ASSIGN_OR_RAISE(auto decimal_scalar, StringToDecimalScalar(raw_number));
        return Builder::MakeDecimalLiteral(decimal_scalar);
      }
      case ValueType::kInterval: {
        auto interval = value.GetValue<ValueType::kInterval>();
        if (interval.months != 0) {
          return arrow::Status::ExecutionError("Interval with months is not supported");
        }
        return Builder::MakeLiteral(interval.micros);
      }
      default:
        return arrow::Status::NotImplemented("RowFilter (ConstNode): value type ",
                                             static_cast<int>(value.GetValueType()), " is not supported");
    }
  }

  ResultType TypedVisit(std::shared_ptr<iceberg::filter::VariableNode> node) {
    auto maybe_arrow_field = arrow_field_resolver_->CreateField(node->column_name);
    if (maybe_arrow_field.ok()) {
      return Builder::MakeField(maybe_arrow_field.ValueUnsafe());
    }
    return ProcessNull(node->value_type);
  }

  template <typename R, ValueType value_type>
  arrow::Result<std::unordered_set<R>> ValuesToUnorderedSet(const iceberg::filter::ArrayHolder& holder) {
    std::unordered_set<R> result;
    for (const auto& maybe_value : holder.GetValue<value_type>()) {
      if (!maybe_value.has_value()) {
        return arrow::Status::ExecutionError("Null not supported");
      }
      result.emplace(maybe_value.value());
    }
    return result;
  }

  ResultType TypedVisit(std::shared_ptr<iceberg::filter::ScalarOverArrayFunctionNode> node) {
    auto func_id = node->function_signature.function_id;
    bool use_or = node->use_or;

    if ((func_id == iceberg::filter::FunctionID::kEqual && use_or) ||
        (func_id == iceberg::filter::FunctionID::kNotEqual && !use_or)) {
      auto try_to_build_in = [this](gandiva::NodePtr evaluated_scalar,
                                    const iceberg::filter::ArrayHolder& array) -> ResultType {
        auto arr_vt = array.GetValueType();
        auto scal_rt = evaluated_scalar->return_type();

        switch (arr_vt) {
          case ValueType::kInt2: {
            if (scal_rt->Equals(arrow::int16())) {
              PromoteToInt4(evaluated_scalar);
              scal_rt = evaluated_scalar->return_type();
            }
            if (scal_rt->Equals(arrow::int32())) {
              ARROW_ASSIGN_OR_RAISE(auto values, (ValuesToUnorderedSet<int32_t, ValueType::kInt2>(array)));
              return Builder::MakeInExpressionInt32(evaluated_scalar, values);
            }
            if (scal_rt->Equals(arrow::int64())) {
              ARROW_ASSIGN_OR_RAISE(auto values, (ValuesToUnorderedSet<int64_t, ValueType::kInt2>(array)));
              return Builder::MakeInExpressionInt64(evaluated_scalar, values);
            }
            break;
          }
          case ValueType::kInt4:
            if (scal_rt->Equals(arrow::int16())) {
              PromoteToInt4(evaluated_scalar);
              scal_rt = evaluated_scalar->return_type();
            }
            if (scal_rt->Equals(arrow::int32())) {
              ARROW_ASSIGN_OR_RAISE(auto values, (ValuesToUnorderedSet<int32_t, ValueType::kInt4>(array)));
              return Builder::MakeInExpressionInt32(evaluated_scalar, values);
            }
            if (scal_rt->Equals(arrow::int64())) {
              ARROW_ASSIGN_OR_RAISE(auto values, (ValuesToUnorderedSet<int64_t, ValueType::kInt4>(array)));
              return Builder::MakeInExpressionInt64(evaluated_scalar, values);
            }
            break;
          case ValueType::kInt8:
            if (scal_rt->Equals(arrow::int32()) || scal_rt->Equals(arrow::int16())) {
              PromoteToInt8(evaluated_scalar);
              scal_rt = evaluated_scalar->return_type();
            }
            if (scal_rt->Equals(arrow::int64())) {
              ARROW_ASSIGN_OR_RAISE(auto values, (ValuesToUnorderedSet<int64_t, ValueType::kInt8>(array)));
              return Builder::MakeInExpressionInt64(evaluated_scalar, values);
            }
            break;
          case ValueType::kFloat4:
            if (scal_rt->Equals(arrow::float32())) {
              ARROW_ASSIGN_OR_RAISE(auto values, (ValuesToUnorderedSet<float, ValueType::kFloat4>(array)));
              return Builder::MakeInExpressionFloat(evaluated_scalar, values);
            }
            if (scal_rt->Equals(arrow::float64())) {
              ARROW_ASSIGN_OR_RAISE(auto values, (ValuesToUnorderedSet<double, ValueType::kFloat4>(array)));
              return Builder::MakeInExpressionDouble(evaluated_scalar, values);
            }
            break;
          case ValueType::kFloat8:
            if (scal_rt->Equals(arrow::float32())) {
              PromoteToFloat8(evaluated_scalar);
              scal_rt = evaluated_scalar->return_type();
            }
            if (scal_rt->Equals(arrow::float64())) {
              ARROW_ASSIGN_OR_RAISE(auto values, (ValuesToUnorderedSet<double, ValueType::kFloat8>(array)));
              return Builder::MakeInExpressionDouble(evaluated_scalar, values);
            }
            break;
          case ValueType::kString:
            if (scal_rt->Equals(arrow::utf8())) {
              ARROW_ASSIGN_OR_RAISE(auto values, (ValuesToUnorderedSet<std::string, ValueType::kString>(array)));
              return Builder::MakeInExpressionString(evaluated_scalar, values);
            }
            break;
          default:
            return arrow::Status::ExecutionError("IN for type ", static_cast<int>(array.GetValueType()),
                                                 " is not supported");
        }
        return arrow::Status::ExecutionError("IN for type ", static_cast<int>(array.GetValueType()),
                                             " is not supported");
      };

      ARROW_ASSIGN_OR_RAISE(auto evaluated_scalar, Visit(node->scalar));

      auto maybe_in = try_to_build_in(evaluated_scalar, node->array);
      if (maybe_in.ok()) {
        auto in_node = maybe_in.ValueUnsafe();
        // in gandiva "null in [...]" is false insicebergd of null
        auto isnull_node = gandiva::TreeExprBuilder::MakeFunction("isnull", {evaluated_scalar}, arrow::boolean());
        auto null_node = gandiva::TreeExprBuilder::MakeNull(arrow::boolean());
        in_node = Builder::MakeIf(isnull_node, null_node, in_node, arrow::boolean());
        if (func_id == iceberg::filter::FunctionID::kNotEqual) {
          in_node = Builder::MakeFunction("not", {in_node}, arrow::boolean());
        }
        return in_node;
      }
    }

    return Visit(iceberg::filter::TreeRewriter::ScalarOverArrayToLogical(node));
  }

  ResultType TypedVisit(std::shared_ptr<iceberg::filter::IfNode> node) {
    ARROW_ASSIGN_OR_RAISE(auto condition_node, Visit(node->condition));
    ARROW_ASSIGN_OR_RAISE(auto then_node, Visit(node->then_node));
    ARROW_ASSIGN_OR_RAISE(auto else_node, Visit(node->else_node));

    if (!then_node->return_type()->Equals(else_node->return_type())) {
      return arrow::Status::ExecutionError("RowFilter (IfNode): ThenNode type '", then_node->return_type()->ToString(),
                                           "' and ElseNode type '", else_node->return_type()->ToString(),
                                           "' are different");
    }

    return Builder::MakeIf(condition_node, then_node, else_node, then_node->return_type());
  }

  ResultType TypedVisit(std::shared_ptr<iceberg::filter::LogicalNode> node) {
    switch (node->operation) {
      case iceberg::filter::LogicalNode::Operation::kNot: {
        if (node->arguments.size() != 1) {
          return arrow::Status::ExecutionError("RowFilter (LogicalNode): not operation expects only 1 argument");
        }

        ARROW_ASSIGN_OR_RAISE(auto arg, Visit(node->arguments[0]));
        return Builder::MakeFunction("not", {arg}, arrow::boolean());
      }

      case iceberg::filter::LogicalNode::Operation::kOr:
      case iceberg::filter::LogicalNode::Operation::kAnd: {
        return MakeLogicalOperation(iceberg::filter::LogicalNode::Operation::kOr == node->operation, node->arguments);
      }
    }
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string("Internal error in iceberg."));
  }

 private:
  ResultType ProcessNull(ValueType type) {
    switch (type) {
      case ValueType::kBool:
        return Builder::MakeNull(arrow::boolean());
      case ValueType::kInt2:
        return Builder::MakeNull(arrow::int16());
      case ValueType::kInt4:
        return Builder::MakeNull(arrow::int32());
      case ValueType::kInt8:
        return Builder::MakeNull(arrow::int64());
      case ValueType::kFloat4:
        return Builder::MakeNull(arrow::float32());
      case ValueType::kFloat8:
        return Builder::MakeNull(arrow::float64());
      case ValueType::kDate:
        return Builder::MakeNull(arrow::date32());
      case ValueType::kTimestamp:
        return Builder::MakeNull(ArrowTimestampType());
      case ValueType::kTimestamptz:
        return Builder::MakeNull(ArrowTimestamptzType());
      case ValueType::kNumeric:
        return Builder::MakeNull(arrow::decimal(1, 0));
      case ValueType::kTime:
        return Builder::MakeNull(arrow::time64(arrow::TimeUnit::MICRO));
      case ValueType::kString:
        return Builder::MakeNull(arrow::utf8());
      case ValueType::kInterval:
        return arrow::Status::ExecutionError("Internal error in iceberg. Null-interval is not expected");
    }
    throw std::runtime_error(__PRETTY_FUNCTION__ + std::string("Internal error in iceberg."));
  }

  ResultType MakeLogicalOperation(bool use_or, const std::vector<iceberg::filter::NodePtr>& node_args) {
    std::vector<gandiva::NodePtr> gandiva_args;
    gandiva_args.reserve(node_args.size());
    for (const auto& arg : node_args) {
      ARROW_ASSIGN_OR_RAISE(auto evaluated_arg, Visit(arg));
      gandiva_args.emplace_back(evaluated_arg);
    }
    if (use_or) {
      return Builder::MakeOr(std::move(gandiva_args));
    } else {
      return Builder::MakeAnd(std::move(gandiva_args));
    }
  }

  std::shared_ptr<const IArrowFieldResolver> arrow_field_resolver_;
  std::shared_ptr<const IGandivaFunctionRegistry> function_registry_;
};

}  // namespace

RowFilter::RowFilter(iceberg::filter::NodePtr root, std::shared_ptr<iceberg::ILogger> logger)
    : root_(root), logger_(logger) {
  if (!root_) {
    throw std::runtime_error("RowFilter: root is not set");
  }
}

arrow::Status RowFilter::BuildFilter(std::shared_ptr<const IArrowFieldResolver> arrow_field_resolver,
                                     std::shared_ptr<const IGandivaFunctionRegistry> function_registry,
                                     std::shared_ptr<arrow::Schema> batch_schema) {
  ARROW_RETURN_NOT_OK(RegisterFunctions());

  RowFilterVisitor visitor(arrow_field_resolver, function_registry);
  ARROW_ASSIGN_OR_RAISE(auto result, visitor.Visit(root_));

  auto condition = Builder::MakeCondition(result);
  if (condition->ToString() == last_condition_as_str_ && batch_schema->ToString() == last_schema_as_str_) {
    return arrow::Status::OK();
  }
  last_schema_as_str_ = batch_schema->ToString();
  last_condition_as_str_ = condition->ToString();
  if (logger_) {
    logger_->Log("row_filter:condition", last_condition_as_str_);
  }
  auto status = gandiva::Filter::Make(batch_schema, condition, &filter_);
  if (!status.ok()) {
    filter_.reset();
    return status;
  }
  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<gandiva::SelectionVector>> RowFilter::ApplyFilter(
    std::shared_ptr<arrow::RecordBatch> record_batch) const {
  if (!filter_) {
    return arrow::Status::ExecutionError("RowFilter (ApplyFilter): filter is not set");
  }

  std::shared_ptr<gandiva::SelectionVector> result_indices;
  ARROW_RETURN_NOT_OK(
      gandiva::SelectionVector::MakeInt32(record_batch->num_rows(), arrow::default_memory_pool(), &result_indices));

  ARROW_RETURN_NOT_OK(filter_->Evaluate(*record_batch, result_indices));

  return result_indices;
}

}  // namespace iceberg::filter
