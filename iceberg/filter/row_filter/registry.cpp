#include "iceberg/filter/row_filter/registry.h"

#include <algorithm>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <variant>

#include "arrow/status.h"
#include "arrow/type_fwd.h"
#include "gandiva/tree_expr_builder.h"
#include "iceberg/filter/representation/function.h"
#include "iceberg/filter/row_filter/arrow_types.h"
#include "iceberg/filter/row_filter/extend_gandiva/register_functions.h"

namespace iceberg::filter {

void GandivaFunctionRegistry::PromoteToInt4(gandiva::NodePtr& gnode) const {
  gnode = gandiva::TreeExprBuilder::MakeFunction("castINT", {gnode}, arrow::int32());
}

void GandivaFunctionRegistry::PromoteToInt8(gandiva::NodePtr& gnode) const {
  gnode = gandiva::TreeExprBuilder::MakeFunction("castBIGINT", {gnode}, arrow::int64());
}

void GandivaFunctionRegistry::PromoteToFloat8(gandiva::NodePtr& gnode) const {
  gnode = gandiva::TreeExprBuilder::MakeFunction("castFLOAT8", {gnode}, arrow::float64());
}

void GandivaFunctionRegistry::PromoteToTimestamp(gandiva::NodePtr& gnode) const {
  auto shift_gnode = gandiva::TreeExprBuilder::MakeLiteral(-timestamp_to_timestamptz_shift_us_);
  gnode = gandiva::TreeExprBuilder::MakeFunction("castTIMESTAMP", {gnode, shift_gnode}, ArrowTimestampType());
}

void GandivaFunctionRegistry::PromoteToTimestamptz(gandiva::NodePtr& gnode) const {
  auto shift_gnode = gandiva::TreeExprBuilder::MakeLiteral(timestamp_to_timestamptz_shift_us_);
  gnode = gandiva::TreeExprBuilder::MakeFunction("castTIMESTAMPTZ", {gnode, shift_gnode}, ArrowTimestamptzType());
}

void GandivaFunctionRegistry::PromoteLtoR(gandiva::NodePtr& a, gandiva::NodePtr& b) const {
  if (a->return_type() == arrow::int16() && b->return_type() == arrow::int32()) {
    PromoteToInt4(a);
  }
  if ((a->return_type() == arrow::int32() || a->return_type() == arrow::int16()) &&
      b->return_type() == arrow::int64()) {
    PromoteToInt8(a);
  }
  if (a->return_type() == arrow::float32() && b->return_type() == arrow::float64()) {
    PromoteToFloat8(a);
  }
  if (a->return_type()->Equals(ArrowTimestamptzType()) &&
      (b->return_type()->Equals(ArrowTimestampType()) || (b->return_type() == arrow::date32()))) {
    PromoteToTimestamp(a);
  }
}

void GandivaFunctionRegistry::Promote(gandiva::NodePtr& l, gandiva::NodePtr& r) const {
  PromoteLtoR(l, r);
  PromoteLtoR(r, l);
}

class TrivialVisitor : public gandiva::NodeVisitor {
 public:
  arrow::Status Visit(const gandiva::FieldNode& node) override {
    return arrow::Status::NotImplemented(__PRETTY_FUNCTION__);
  }
  arrow::Status Visit(const gandiva::FunctionNode& node) override {
    return arrow::Status::NotImplemented(__PRETTY_FUNCTION__);
  }
  arrow::Status Visit(const gandiva::IfNode& node) override {
    return arrow::Status::NotImplemented(__PRETTY_FUNCTION__);
  }
  arrow::Status Visit(const gandiva::LiteralNode& node) override {
    return arrow::Status::NotImplemented(__PRETTY_FUNCTION__);
  }
  arrow::Status Visit(const gandiva::BooleanNode& node) override {
    return arrow::Status::NotImplemented(__PRETTY_FUNCTION__);
  }
  arrow::Status Visit(const gandiva::InExpressionNode<int32_t>& node) override {
    return arrow::Status::NotImplemented(__PRETTY_FUNCTION__);
  }
  arrow::Status Visit(const gandiva::InExpressionNode<int64_t>& node) override {
    return arrow::Status::NotImplemented(__PRETTY_FUNCTION__);
  }
  arrow::Status Visit(const gandiva::InExpressionNode<float>& node) override {
    return arrow::Status::NotImplemented(__PRETTY_FUNCTION__);
  }
  arrow::Status Visit(const gandiva::InExpressionNode<double>& node) override {
    return arrow::Status::NotImplemented(__PRETTY_FUNCTION__);
  }
  arrow::Status Visit(const gandiva::InExpressionNode<gandiva::DecimalScalar128>& node) override {
    return arrow::Status::NotImplemented(__PRETTY_FUNCTION__);
  }
  arrow::Status Visit(const gandiva::InExpressionNode<std::string>& node) override {
    return arrow::Status::NotImplemented(__PRETTY_FUNCTION__);
  }
};

class LikePatternValidator : public TrivialVisitor {
 public:
  arrow::Status Visit(const gandiva::LiteralNode& node) override {
    if (node.is_null()) {
      return arrow::Status::ExecutionError("LikePatternValidator: null is not expected");
    }

    if (!std::holds_alternative<std::string>(node.holder())) {
      return arrow::Status::ExecutionError("LikePatternValidator: string expected");
    }

    const std::string& pattern = std::get<std::string>(node.holder());
    if (std::find(pattern.begin(), pattern.end(), '\\') != pattern.end()) {
      return arrow::Status::ExecutionError("LikePatternValidator: escape characters are not supported");
    }

    return arrow::Status::OK();
  }
};

class TimeUnitExtractor : public TrivialVisitor {
 public:
  arrow::Status Visit(const gandiva::LiteralNode& node) override {
    if (node.is_null()) {
      return arrow::Status::ExecutionError("TimeUnitExtractor: null is not expected");
    }

    if (!std::holds_alternative<std::string>(node.holder())) {
      return arrow::Status::ExecutionError("TimeUnitExtractor: string expected");
    }

    time_unit_.emplace(std::get<std::string>(node.holder()));
    return arrow::Status::OK();
  }

  arrow::Result<std::string> ExtractTimeUnit() const {
    if (!time_unit_.has_value()) {
      return arrow::Status::ExecutionError("TimeUnitExtractor: time unit is not set");
    }
    return time_unit_.value();
  }

 private:
  std::optional<std::string> time_unit_;
};

static bool IsComparison(iceberg::filter::FunctionID id) {
  using ID = iceberg::filter::FunctionID;

  switch (id) {
    case ID::kLessThan:
    case ID::kLessThanOrEqualTo:
    case ID::kGreaterThan:
    case ID::kGreaterThanOrEqualTo:
    case ID::kEqual:
    case ID::kNotEqual:
      return true;
    default:
      return false;
  }
}

static bool IsArithmetic(iceberg::filter::FunctionID id) {
  using ID = iceberg::filter::FunctionID;

  switch (id) {
    case ID::kAddWithChecks:
    case ID::kAddWithoutChecks:
    case ID::kSubtractWithoutChecks:
    case ID::kSubtractWithChecks:
    case ID::kMultiplyWithoutChecks:
    case ID::kMultiplyWithChecks:
    case ID::kDivideWithoutChecks:
    case ID::kDivideWithChecks:
    case ID::kModuloWithChecks:
      return true;
    default:
      return false;
  }
}

arrow::Result<gandiva::NodePtr> GandivaFunctionRegistry::CreateFunction(iceberg::filter::FunctionID function_id,
                                                                        gandiva::NodeVector arguments) const {
  ARROW_RETURN_NOT_OK(RegisterFunctions());
  if (arguments.size() == 2 && (IsComparison(function_id) || IsArithmetic(function_id))) {
    Promote(arguments[0], arguments[1]);
  }

  using B = gandiva::TreeExprBuilder;
  using ID = iceberg::filter::FunctionID;

  // TODO(gmusya): maybe validate arguments for each function
  switch (function_id) {
    case ID::kLessThan:
      return B::MakeFunction("less_than", arguments, arrow::boolean());
    case ID::kLessThanOrEqualTo:
      return B::MakeFunction("less_than_or_equal_to", arguments, arrow::boolean());
    case ID::kGreaterThan:
      return B::MakeFunction("greater_than", arguments, arrow::boolean());
    case ID::kGreaterThanOrEqualTo:
      return B::MakeFunction("greater_than_or_equal_to", arguments, arrow::boolean());
    case ID::kEqual:
      return B::MakeFunction("equal", arguments, arrow::boolean());
    case ID::kNotEqual:
      return B::MakeFunction("not_equal", arguments, arrow::boolean());
    case ID::kLike:
    case ID::kNotLike: {
      LikePatternValidator validator;
      ARROW_RETURN_NOT_OK(arguments[1]->Accept(validator));
      arguments.emplace_back(
          std::make_shared<gandiva::LiteralNode>(arrow::utf8(), gandiva::LiteralHolder(std::string(1, '\\')), false));
      auto result = B::MakeFunction("like", arguments, arrow::boolean());
      if (function_id == ID::kNotLike) {
        result = B::MakeFunction("not", {result}, arrow::boolean());
      }
      return result;
    }
    case ID::kILike:
    case ID::kNotILike: {
      LikePatternValidator validator;
      ARROW_RETURN_NOT_OK(arguments[1]->Accept(validator));
      auto result = B::MakeFunction("ilike", arguments, arrow::boolean());
      if (function_id == ID::kNotILike) {
        result = B::MakeFunction("not", {result}, arrow::boolean());
      }
      return result;
    }

    case ID::kAddWithChecks:
      return B::MakeFunction("AddOverflow", arguments, arguments[0]->return_type());
    case ID::kSubtractWithChecks:
      return B::MakeFunction("SubOverflow", arguments, arguments[0]->return_type());
    case ID::kMultiplyWithChecks:
      return B::MakeFunction("MulOverflow", arguments, arguments[0]->return_type());
    case ID::kDivideWithChecks:
      return B::MakeFunction("DivSafe", arguments, arguments[0]->return_type());
    case ID::kModuloWithChecks:
      return B::MakeFunction("ModSafe", arguments, arguments[0]->return_type());
    case ID::kAddWithoutChecks:
      return B::MakeFunction("add", arguments, arguments[0]->return_type());
    case ID::kSubtractWithoutChecks:
      return B::MakeFunction("subtract", arguments, arguments[0]->return_type());
    case ID::kMultiplyWithoutChecks:
      return B::MakeFunction("multiply", arguments, arguments[0]->return_type());
    case ID::kDivideWithoutChecks:
      return B::MakeFunction("divide", arguments, arguments[0]->return_type());
    case ID::kBitwiseAnd:
      return B::MakeFunction("bitwise_and", arguments, arguments[0]->return_type());
    case ID::kBitwiseOr:
      return B::MakeFunction("bitwise_or", arguments, arguments[0]->return_type());
    case ID::kBitwiseNot:
      return B::MakeFunction("bitwise_not", arguments, arguments[0]->return_type());
    case ID::kXor:
      return B::MakeFunction("xor", arguments, arguments[0]->return_type());
    case ID::kAbsolute:
      return B::MakeFunction("abs", arguments, arguments[0]->return_type());
    case ID::kNegative:
      return B::MakeFunction("negative", arguments, arguments[0]->return_type());
    case ID::kSign:
      return B::MakeFunction("sign", arguments, arguments[0]->return_type());
    case ID::kCeil:
      return B::MakeFunction("ceil", arguments, arguments[0]->return_type());
    case ID::kFloor:
      return B::MakeFunction("floor", arguments, arguments[0]->return_type());
    case ID::kRound:
      return B::MakeFunction("BankersRound", arguments, arguments[0]->return_type());
    case ID::kSqrt:
      return B::MakeFunction("sqrt", arguments, arguments[0]->return_type());
    case ID::kCbrt:
      return B::MakeFunction("cbrt", arguments, arguments[0]->return_type());
    case ID::kExp:
      return B::MakeFunction("exp", arguments, arguments[0]->return_type());
    case ID::kLog10:
      return B::MakeFunction("log10", arguments, arguments[0]->return_type());
    case ID::kSin:
      return B::MakeFunction("sin", arguments, arguments[0]->return_type());
    case ID::kCos:
      return B::MakeFunction("cos", arguments, arguments[0]->return_type());
    case ID::kTan:
      return B::MakeFunction("tan", arguments, arguments[0]->return_type());
    case ID::kCot:
      return B::MakeFunction("cot", arguments, arguments[0]->return_type());
    case ID::kAtan:
      return B::MakeFunction("atan", arguments, arguments[0]->return_type());
    case ID::kAtan2:
      return B::MakeFunction("atan2", arguments, arguments[0]->return_type());
    case ID::kAsin:
      return B::MakeFunction("asin", arguments, arguments[0]->return_type());
    case ID::kAcos:
      return B::MakeFunction("acos", arguments, arguments[0]->return_type());
    case ID::kSinh:
      return B::MakeFunction("sinh", arguments, arguments[0]->return_type());
    case ID::kCosh:
      return B::MakeFunction("cosh", arguments, arguments[0]->return_type());
    case ID::kTanh:
      return B::MakeFunction("tanh", arguments, arguments[0]->return_type());
    case ID::kCharLength:
      return B::MakeFunction("char_length", arguments, arrow::int32());
    case ID::kLower:
      return B::MakeFunction("lower", arguments, arguments[0]->return_type());
    case ID::kUpper:
      return B::MakeFunction("upper", arguments, arguments[0]->return_type());
    case ID::kLTrim:
      return B::MakeFunction("ltrim", arguments, arguments[0]->return_type());
    case ID::kRTrim:
      return B::MakeFunction("rtrim", arguments, arguments[0]->return_type());
    case ID::kBTrim:
      return B::MakeFunction("btrim", arguments, arguments[0]->return_type());
    case ID::kSubstring:
      for (size_t i = 1; i < arguments.size(); ++i) {
        PromoteToInt8(arguments[i]);
      }
      return B::MakeFunction("substring", arguments, arguments[0]->return_type());
    case ID::kLocate:
      std::swap(arguments[0], arguments[1]);
      return B::MakeFunction("locate", arguments, arrow::int32());
    case ID::kConcatenate:
      return B::MakeFunction("concatOperator", arguments, arguments[0]->return_type());
    case ID::kIsNull:
      if (arguments[0]->return_type()->Equals(ArrowTimestamptzType())) {
        PromoteToTimestamp(arguments[0]);
      }
      return B::MakeFunction("isnull", arguments, arrow::boolean());
    case ID::kCastInt8:
      return B::MakeFunction("castBIGINT", arguments, arrow::int64());
    case ID::kCastInt4:
      return B::MakeFunction("castINT", arguments, arrow::int32());
    case ID::kCastFloat8:
      return B::MakeFunction("castFLOAT8", arguments, arrow::float64());
    case ID::kDateDiff:
      return B::MakeFunction("DateDiff", arguments, arrow::int32());
    case ID::kExtractTimestamp: {
      TimeUnitExtractor extractor;
      ARROW_RETURN_NOT_OK(arguments[0]->Accept(extractor));
      ARROW_ASSIGN_OR_RAISE(std::string unit, extractor.ExtractTimeUnit());
      std::transform(unit.begin(), unit.end(), unit.begin(), [](char ch) { return std::tolower(ch); });
      static const std::set<std::string> kSupportedFields = {
          "millennium", "century", "decade", "year",   "quarter",      "month",       "week",
          "day",        "hour",    "minute", "second", "milliseconds", "microseconds"};
      if (kSupportedFields.contains(unit)) {
        unit[0] = toupper(unit[0]);
        std::string function_name = "Extract" + unit;
        return B::MakeFunction(function_name, {arguments[1]}, arrow::float64());
      }
      return arrow::Status::ExecutionError("ExtractTimestamp for ", unit, " is not supported");
    }
    case ID::kDateTrunc: {
      TimeUnitExtractor extractor;
      ARROW_RETURN_NOT_OK(arguments[0]->Accept(extractor));
      ARROW_ASSIGN_OR_RAISE(std::string unit, extractor.ExtractTimeUnit());
      std::transform(unit.begin(), unit.end(), unit.begin(), [](char ch) { return std::tolower(ch); });

      static const std::set<std::string> kSupportedFields = {"millennium", "century", "decade", "year",
                                                             "quarter",    "month",   "week",   "day",
                                                             "hour",       "minute",  "second", "milliseconds"};
      if (kSupportedFields.contains(unit)) {
        unit[0] = toupper(unit[0]);
        std::string function_name = "DateTrunc" + unit;
        return B::MakeFunction(function_name, {arguments[1]}, arguments[1]->return_type());
      }
      return arrow::Status::ExecutionError("DateTrunc for ", unit, " is not supported");
    }
    case ID::kCastTimestamp:
      PromoteToTimestamp(arguments[0]);
      return arguments[0];
    case ID::kCastTimestamptz:
      PromoteToTimestamptz(arguments[0]);
      return arguments[0];
    case ID::kCastDate: {
      if (arguments[0]->return_type()->Equals(ArrowTimestamptzType())) {
        PromoteToTimestamp(arguments[0]);
      }
      return B::MakeFunction("castDATE", arguments, arrow::date32());
    }
    default:
      return arrow::Status::NotImplemented("GandivaFunctionRegistry::CreateFunction: ", static_cast<int>(function_id));
  }
}

}  // namespace iceberg::filter
