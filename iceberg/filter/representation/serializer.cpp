#include "iceberg/filter/representation/serializer.h"

#include <memory>
#include <stdexcept>
#include <utility>
#include <variant>
#include <vector>

#include "arrow/status.h"
#include "iceberg/common/error.h"
#include "iceberg/filter/representation/function.h"
#include "iceberg/filter/representation/node.h"
#include "iceberg/filter/representation/value.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace iceberg::filter {
namespace serializer {

namespace field {
constexpr std::string_view kNodeType = "node-type";
constexpr std::string_view kFunctionSignature = "func-sig";
constexpr std::string_view kArgumentTypes = "arg-types";
constexpr std::string_view kFunctionID = "func-id";
constexpr std::string_view kLogicalOperation = "logic-op";
constexpr std::string_view kArguments = "args";
constexpr std::string_view kColumnName = "col-name";
constexpr std::string_view kValueType = "val-type";
constexpr std::string_view kValue = "value";
constexpr std::string_view kCondition = "cond";
constexpr std::string_view kThen = "then";
constexpr std::string_view kElse = "else";
constexpr std::string_view kUseOr = "use-or";
constexpr std::string_view kLhs = "lhs";
constexpr std::string_view kRhs = "rhs";
constexpr std::string_view kMonths = "months";
constexpr std::string_view kMicros = "micros";
}  // namespace field

template <typename T>
constexpr bool IsBijection(const T& map) {
  for (const auto& [lhs_1, lhs_2] : map) {
    for (const auto& [rhs_1, rhs_2] : map) {
      bool eq_1 = lhs_1 == rhs_1;
      bool eq_2 = lhs_2 == rhs_2;
      if (eq_1 != eq_2) {
        return false;
      }
    }
  }
  return true;
}

template <typename MapType, typename ElemType>
constexpr std::string_view ElementToStringView(const MapType& map, ElemType elem) {
  for (const auto& [possible_elem, possible_str] : map) {
    if (elem == possible_elem) {
      return possible_str;
    }
  }
  throw arrow::Status::ExecutionError(__PRETTY_FUNCTION__, ": unexpected node type ", static_cast<int>(elem));
}

template <typename MapType>
constexpr auto StringViewToElement(const MapType& map, std::string_view elem) {
  for (const auto& [possible_elem, possible_str] : map) {
    if (elem == possible_str) {
      return possible_elem;
    }
  }
  throw arrow::Status::ExecutionError(__PRETTY_FUNCTION__, ": unexpected string_view ", elem);
}

struct NodeTypeString {
  NodeType node_type;
  std::string_view str;
};
constexpr NodeTypeString nodetype_string[] = {{NodeType::kVariable, "var"},
                                              {NodeType::kConst, "const"},
                                              {NodeType::kFunction, "func"},
                                              {NodeType::kIf, "if"},
                                              {NodeType::kScalarOverArrayFunction, "funcarr"},
                                              {NodeType::kLogical, "logical"}};
static_assert(IsBijection(nodetype_string));

constexpr std::string_view NodeTypeToString(NodeType node_type) {
  return ElementToStringView(nodetype_string, node_type);
}

constexpr NodeType StringToNodeType(std::string_view string) { return StringViewToElement(nodetype_string, string); }

struct FunctionIDString {
  FunctionID function_id;
  std::string_view str;
};
constexpr FunctionIDString functionid_string[] = {{FunctionID::kLessThan, "kLessThan"},
                                                  {FunctionID::kGreaterThan, "kGreaterThan"},
                                                  {FunctionID::kLessThanOrEqualTo, "kLessThanOrEqualTo"},
                                                  {FunctionID::kGreaterThanOrEqualTo, "kGreaterThanOrEqualTo"},
                                                  {FunctionID::kEqual, "kEqual"},
                                                  {FunctionID::kNotEqual, "kNotEqual"},
                                                  {FunctionID::kAddWithChecks, "kAddWithChecks"},
                                                  {FunctionID::kSubtractWithChecks, "kSubtractWithChecks"},
                                                  {FunctionID::kMultiplyWithChecks, "kMultiplyWithChecks"},
                                                  {FunctionID::kDivideWithChecks, "kDivideWithChecks"},
                                                  {FunctionID::kModuloWithChecks, "kModuloWithChecks"},
                                                  {FunctionID::kAddWithoutChecks, "kAddWithoutChecks"},
                                                  {FunctionID::kSubtractWithoutChecks, "kSubtractWithoutChecks"},
                                                  {FunctionID::kMultiplyWithoutChecks, "kMultiplyWithoutChecks"},
                                                  {FunctionID::kDivideWithoutChecks, "kDivideWithoutChecks"},
                                                  {FunctionID::kBitwiseAnd, "kBitwiseAnd"},
                                                  {FunctionID::kBitwiseOr, "kBitwiseOr"},
                                                  {FunctionID::kXor, "kXor"},
                                                  {FunctionID::kBitwiseNot, "kBitwiseNot"},
                                                  {FunctionID::kAbsolute, "kAbsolute"},
                                                  {FunctionID::kNegative, "kNegative"},
                                                  {FunctionID::kLike, "kLike"},
                                                  {FunctionID::kILike, "kILike"},
                                                  {FunctionID::kNotLike, "kNotLike"},
                                                  {FunctionID::kNotILike, "kNotILike"},
                                                  {FunctionID::kSign, "kSign"},
                                                  {FunctionID::kCeil, "kCeil"},
                                                  {FunctionID::kFloor, "kFloor"},
                                                  {FunctionID::kRound, "kRound"},
                                                  {FunctionID::kSqrt, "kSqrt"},
                                                  {FunctionID::kCbrt, "kCbrt"},
                                                  {FunctionID::kExp, "kExp"},
                                                  {FunctionID::kLog10, "kLog10"},
                                                  {FunctionID::kSin, "kSin"},
                                                  {FunctionID::kCos, "kCos"},
                                                  {FunctionID::kTan, "kTan"},
                                                  {FunctionID::kCot, "kCot"},
                                                  {FunctionID::kAtan, "kAtan"},
                                                  {FunctionID::kAtan2, "kAtan2"},
                                                  {FunctionID::kSinh, "kSinh"},
                                                  {FunctionID::kCosh, "kCosh"},
                                                  {FunctionID::kTanh, "kTanh"},
                                                  {FunctionID::kAsin, "kAsin"},
                                                  {FunctionID::kAcos, "kAcos"},
                                                  {FunctionID::kCharLength, "kCharLength"},
                                                  {FunctionID::kLower, "kLower"},
                                                  {FunctionID::kUpper, "kUpper"},
                                                  {FunctionID::kLTrim, "kLTrim"},
                                                  {FunctionID::kRTrim, "kRTrim"},
                                                  {FunctionID::kBTrim, "kBTrim"},
                                                  {FunctionID::kSubstring, "kSubstring"},
                                                  {FunctionID::kLocate, "kLocate"},
                                                  {FunctionID::kIsNull, "kIsNull"},
                                                  {FunctionID::kConcatenate, "kConcatenate"},
                                                  {FunctionID::kExtractTimestamp, "kExtractTimestamp"},
                                                  {FunctionID::kCastTimestamp, "kCastTimestamp"},
                                                  {FunctionID::kCastTimestamptz, "kCastTimestamptz"},
                                                  {FunctionID::kDateDiff, "kDateDiff"},
                                                  {FunctionID::kDateTrunc, "kDateTrunc"},
                                                  {FunctionID::kCastFloat8, "kCastFloat8"},
                                                  {FunctionID::kCastInt8, "kCastInt8"},
                                                  {FunctionID::kCastInt4, "kCastInt4"},
                                                  {FunctionID::kCastDate, "kCastDate"}};
static_assert(IsBijection(functionid_string));

constexpr std::string_view FunctionIDToString(FunctionID func_id) {
  return ElementToStringView(functionid_string, func_id);
}

constexpr FunctionID StringToFunctionID(std::string_view string) {
  return StringViewToElement(functionid_string, string);
}

struct ValueTypeString {
  ValueType value_type;
  std::string_view str;
};
constexpr ValueTypeString valuetype_string[] = {{ValueType::kBool, "bool"},
                                                {ValueType::kInt2, "int2"},
                                                {ValueType::kInt4, "int4"},
                                                {ValueType::kInt8, "int8"},
                                                {ValueType::kFloat4, "float4"},
                                                {ValueType::kFloat8, "float8"},
                                                {ValueType::kNumeric, "numeric"},
                                                {ValueType::kString, "string"},
                                                {ValueType::kDate, "date"},
                                                {ValueType::kTimestamp, "timestamp"},
                                                {ValueType::kTimestamptz, "timestamptz"},
                                                {ValueType::kTime, "time"},
                                                {ValueType::kInterval, "interval"}};
static_assert(IsBijection(valuetype_string));

constexpr std::string_view ValueTypeToString(ValueType value_type) {
  return ElementToStringView(valuetype_string, value_type);
}

constexpr ValueType StringToValueType(std::string_view string) { return StringViewToElement(valuetype_string, string); }

struct LogicalOperationString {
  LogicalNode::Operation function_id;
  std::string_view str;
};
constexpr LogicalOperationString logicalop_string[] = {{LogicalNode::Operation::kAnd, "kAnd"},
                                                       {LogicalNode::Operation::kOr, "kOr"},
                                                       {LogicalNode::Operation::kNot, "kNot"}};
static_assert(IsBijection(logicalop_string));

constexpr std::string_view LogicalOperationToString(LogicalNode::Operation func_id) {
  return ElementToStringView(logicalop_string, func_id);
}

constexpr LogicalNode::Operation StringToLogicalOperation(std::string_view string) {
  return StringViewToElement(logicalop_string, string);
}

using Allocator = rapidjson::MemoryPoolAllocator<rapidjson::CrtAllocator>;

class Serializer {
 public:
  rapidjson::Value Serialize(const NodePtr&);

 private:
  // clang-format off
  template <typename T>
  rapidjson::Value Serialize(const T&)
  requires(std::is_same_v<std::vector<typename T::value_type>, T>);

  template <typename T>
  rapidjson::Value Serialize(const T&)
  requires(std::is_same_v<std::optional<typename T::value_type>, T>);
  // clang-format on

  rapidjson::Value Serialize(NodeType);
  rapidjson::Value Serialize(ValueType);
  rapidjson::Value Serialize(FunctionID);
  rapidjson::Value Serialize(LogicalNode::Operation);
  rapidjson::Value Serialize(const FunctionSignature&);

  rapidjson::Value Serialize(const std::shared_ptr<VariableNode>&);
  rapidjson::Value Serialize(const std::shared_ptr<LogicalNode>&);
  rapidjson::Value Serialize(const std::shared_ptr<IfNode>&);
  rapidjson::Value Serialize(const std::shared_ptr<FunctionNode>&);
  rapidjson::Value Serialize(const std::shared_ptr<ConstNode>&);
  rapidjson::Value Serialize(const std::shared_ptr<ScalarOverArrayFunctionNode>&);

  rapidjson::Value Serialize(const Numeric&);
  rapidjson::Value Serialize(std::same_as<bool> auto);
  rapidjson::Value Serialize(std::same_as<int16_t> auto);
  rapidjson::Value Serialize(std::same_as<int32_t> auto);
  rapidjson::Value Serialize(std::same_as<int64_t> auto);
  rapidjson::Value Serialize(std::same_as<float> auto);
  rapidjson::Value Serialize(std::same_as<double> auto);
  rapidjson::Value Serialize(std::string_view);
  rapidjson::Value Serialize(const std::string&);
  rapidjson::Value Serialize(const Value&);
  rapidjson::Value Serialize(const ArrayHolder&);
  rapidjson::Value Serialize(const IntervalMonthMicro&);

 private:
  void AddNodeType(rapidjson::Value& result, NodeType node_type);
  void AddMember(rapidjson::Value& result, std::string_view key, rapidjson::Value&& value);

  Allocator allocator_;
};

void Serializer::AddNodeType(rapidjson::Value& result, NodeType node_type) {
  AddMember(result, field::kNodeType, Serialize(node_type));
}

void Serializer::AddMember(rapidjson::Value& result, std::string_view key, rapidjson::Value&& value) {
  result.AddMember(rapidjson::Value(key.data(), allocator_), std::move(value), allocator_);
}

rapidjson::Value Serializer::Serialize(std::same_as<bool> auto value) { return rapidjson::Value(value); }
rapidjson::Value Serializer::Serialize(std::same_as<int64_t> auto value) { return rapidjson::Value(value); }
rapidjson::Value Serializer::Serialize(std::same_as<int32_t> auto value) { return rapidjson::Value(value); }
rapidjson::Value Serializer::Serialize(std::same_as<int16_t> auto value) { return rapidjson::Value(value); }
rapidjson::Value Serializer::Serialize(std::same_as<float> auto value) { return rapidjson::Value(value); }
rapidjson::Value Serializer::Serialize(std::same_as<double> auto value) { return rapidjson::Value(value); }
rapidjson::Value Serializer::Serialize(std::string_view sv) { return rapidjson::Value(sv.data(), allocator_); }
rapidjson::Value Serializer::Serialize(const std::string& sv) { return rapidjson::Value(sv.data(), allocator_); }

rapidjson::Value Serializer::Serialize(const Numeric& numeric) { return Serialize(numeric.value); }
rapidjson::Value Serializer::Serialize(NodeType node_type) { return Serialize(NodeTypeToString(node_type)); }
rapidjson::Value Serializer::Serialize(ValueType value_type) {
  return Serialize(serializer::ValueTypeToString(value_type));
}
rapidjson::Value Serializer::Serialize(FunctionID func_id) { return Serialize(FunctionIDToString(func_id)); }
rapidjson::Value Serializer::Serialize(LogicalNode::Operation node_type) {
  return Serialize(LogicalOperationToString(node_type));
}

rapidjson::Value Serializer::Serialize(const IntervalMonthMicro& interval) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddMember(result, field::kMonths, Serialize(interval.months));
  AddMember(result, field::kMicros, Serialize(interval.micros));
  return result;
}

// clang-format off
template <typename T>
rapidjson::Value Serializer::Serialize(const T& maybe_value)
requires(std::is_same_v<std::optional<typename T::value_type>, T>) {
  if (maybe_value.has_value()) {
    return Serialize(maybe_value.value());
  } else {
    return rapidjson::Value(rapidjson::kNullType);
  }
}

template <typename T>
rapidjson::Value Serializer::Serialize(const T& arr)
requires(std::is_same_v<std::vector<typename T::value_type>, T>) {
  rapidjson::Value result(rapidjson::kArrayType);
  for (const auto& arg : arr) {
    result.PushBack(Serialize(arg), allocator_);
  }
  return result;
}
// clang-format on

rapidjson::Value Serializer::Serialize(const ArrayHolder& value) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddMember(result, field::kValueType, Serialize(value.GetValueType()));
  AddMember(result, field::kValue, std::visit([&](auto&& arg) { return Serialize(arg); }, value.GetHolder()));
  return result;
}

rapidjson::Value Serializer::Serialize(const Value& value) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddMember(result, field::kValueType, Serialize(value.GetValueType()));
  AddMember(result, field::kValue, std::visit([&](auto&& arg) { return Serialize(arg); }, value.GetHolder()));
  return result;
}

rapidjson::Value Serializer::Serialize(const NodePtr& node) {
  switch (node->node_type) {
    case NodeType::kVariable:
      return Serialize(std::static_pointer_cast<VariableNode>(node));
    case NodeType::kConst:
      return Serialize(std::static_pointer_cast<ConstNode>(node));
    case NodeType::kFunction:
      return Serialize(std::static_pointer_cast<FunctionNode>(node));
    case NodeType::kIf:
      return Serialize(std::static_pointer_cast<IfNode>(node));
    case NodeType::kScalarOverArrayFunction:
      return Serialize(std::static_pointer_cast<ScalarOverArrayFunctionNode>(node));
    case NodeType::kLogical:
      return Serialize(std::static_pointer_cast<LogicalNode>(node));
  }
  throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": internal error"));
}

rapidjson::Value Serializer::Serialize(const FunctionSignature& signature) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddMember(result, field::kArgumentTypes, Serialize(signature.argument_types));
  AddMember(result, field::kValueType, Serialize(signature.return_type));
  AddMember(result, field::kFunctionID, Serialize(signature.function_id));
  return result;
}

rapidjson::Value Serializer::Serialize(const std::shared_ptr<LogicalNode>& node) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddNodeType(result, NodeType::kLogical);
  AddMember(result, field::kLogicalOperation, Serialize(node->operation));
  AddMember(result, field::kArguments, Serialize(node->arguments));
  return result;
}

rapidjson::Value Serializer::Serialize(const std::shared_ptr<FunctionNode>& node) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddNodeType(result, NodeType::kFunction);
  AddMember(result, field::kFunctionSignature, Serialize(node->function_signature));
  AddMember(result, field::kArguments, Serialize(node->arguments));
  return result;
}

rapidjson::Value Serializer::Serialize(const std::shared_ptr<VariableNode>& node) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddNodeType(result, NodeType::kVariable);
  AddMember(result, field::kValueType, Serialize(node->value_type));
  AddMember(result, field::kColumnName, Serialize(node->column_name));
  return result;
}

rapidjson::Value Serializer::Serialize(const std::shared_ptr<ConstNode>& node) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddNodeType(result, NodeType::kConst);
  AddMember(result, field::kValue, Serialize(node->value));
  return result;
}

rapidjson::Value Serializer::Serialize(const std::shared_ptr<IfNode>& node) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddNodeType(result, NodeType::kIf);
  AddMember(result, field::kCondition, Serialize(node->condition));
  AddMember(result, field::kThen, Serialize(node->then_node));
  AddMember(result, field::kElse, Serialize(node->else_node));
  return result;
}

rapidjson::Value Serializer::Serialize(const std::shared_ptr<ScalarOverArrayFunctionNode>& node) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddNodeType(result, NodeType::kScalarOverArrayFunction);
  AddMember(result, field::kFunctionSignature, Serialize(node->function_signature));
  AddMember(result, field::kUseOr, Serialize(node->use_or));
  AddMember(result, field::kLhs, Serialize(node->scalar));
  AddMember(result, field::kRhs, Serialize(node->array));
  return result;
}

template <typename T>
T Deserialize(const rapidjson::Value& document) {
  Ensure(document.Is<T>(), "Deserialize (primitive): wrong type");
  return document.Get<T>();
}

template <>
int16_t Deserialize(const rapidjson::Value& document) {
  // there is no document.Is<int16_t>() method
  Ensure(document.Is<int32_t>(), "Deserialize (primitive): wrong type");
  // TODO(gmusya): check if value fits in int16_t
  return document.Get<int32_t>();
}

// clang-format off
template <typename T>
T Deserialize(const rapidjson::Value& document)
requires(std::is_same_v<std::vector<typename T::value_type>, T>);

template <typename T>
T Deserialize(const rapidjson::Value& document)
requires(std::is_same_v<std::optional<typename T::value_type>, T>) {
  if (document.IsNull()) {
    return std::nullopt;
  } else {
    return Deserialize<typename T::value_type>(document);
  }
}
// clang-format on

// clang-format off
template <typename T>
T Deserialize(const rapidjson::Value& document)
requires(std::is_same_v<std::vector<typename T::value_type>, T>) {
  Ensure(document.IsArray(), "Deserialize (array): wrong type");

  T result;
  for (const auto& element : document.GetArray()) {
    result.emplace_back(Deserialize<typename T::value_type>(element));
  }
  return result;
}
// clang-format on

template <>
std::string Deserialize(const rapidjson::Value& document) {
  Ensure(document.IsString(), "Deserialize (string): wrong type");
  return std::string(document.GetString(), document.GetStringLength());
}

template <>
Numeric Deserialize(const rapidjson::Value& document) {
  return Numeric{Deserialize<std::string>(document)};
}

template <typename T>
T Extract(const rapidjson::Value& document, std::string_view field_name) {
  const char* c_str = field_name.data();
  Ensure(document.HasMember(c_str), "ExtractPrimitive: !document.HasMember(" + std::string(field_name) + ")");
  return Deserialize<T>(document[c_str]);
}

template <>
IntervalMonthMicro Deserialize(const rapidjson::Value& document) {
  Ensure(document.IsObject(), "Deserialize (IntervalMonthMicro): wrong type");
  int64_t months = Extract<int64_t>(document, field::kMonths);
  int64_t micros = Extract<int64_t>(document, field::kMicros);
  return IntervalMonthMicro{.months = months, .micros = micros};
}

template <>
ArrayHolder Deserialize(const rapidjson::Value& document) {
  Ensure(document.IsObject(), std::string(__PRETTY_FUNCTION__) + ": wrong type");

  ValueType value_type = StringToValueType(Extract<std::string>(document, field::kValueType));

  return std::visit(
      [&]<ValueType value_type>(Tag<value_type>) -> ArrayHolder {
        return ArrayHolder::Make<value_type>(Extract<Array<value_type>>(document, field::kValue));
      },
      DispatchTag(value_type));
}

template <>
Value Deserialize(const rapidjson::Value& document) {
  Ensure(document.IsObject(), std::string(__PRETTY_FUNCTION__) + ": wrong type");
  ValueType value_type = StringToValueType(Extract<std::string>(document, field::kValueType));

  return std::visit(
      [&]<ValueType value_type>(Tag<value_type>) -> Value {
        return Value::Make<value_type>(Extract<PhysicalNullableType<value_type>>(document, field::kValue));
      },
      DispatchTag(value_type));
}

template <>
std::shared_ptr<ConstNode> Deserialize(const rapidjson::Value& document) {
  Ensure(document.IsObject(), "Deserialize (ConstNode): wrong type");
  Value value = Extract<Value>(document, field::kValue);
  return std::make_shared<ConstNode>(std::move(value));
}

template <>
ValueType Deserialize(const rapidjson::Value& document) {
  Ensure(document.IsString(), "Deserialize (ValueType): wrong type");
  return StringToValueType(document.GetString());
}

template <>
FunctionSignature Deserialize(const rapidjson::Value& document) {
  Ensure(document.IsObject(), "Deserialize (FunctionSignature): wrong type");

  FunctionID function_id = StringToFunctionID(Extract<std::string>(document, field::kFunctionID));
  ValueType return_type = StringToValueType(Extract<std::string>(document, field::kValueType));
  std::vector<ValueType> arg_types = Extract<std::vector<ValueType>>(document, field::kArgumentTypes);

  return FunctionSignature{
      .function_id = function_id, .return_type = return_type, .argument_types = std::move(arg_types)};
}

template <>
std::shared_ptr<LogicalNode> Deserialize(const rapidjson::Value& document) {
  Ensure(document.IsObject(), "Deserialize (LogicalNode): wrong type");

  LogicalNode::Operation op = StringToLogicalOperation(Extract<std::string>(document, field::kLogicalOperation));
  auto arguments = Extract<std::vector<NodePtr>>(document, field::kArguments);

  return std::make_shared<LogicalNode>(op, std::move(arguments));
}

template <>
std::shared_ptr<FunctionNode> Deserialize(const rapidjson::Value& document) {
  Ensure(document.IsObject(), "Deserialize (FunctionNode): wrong type");

  FunctionSignature funciton_signature = Extract<FunctionSignature>(document, field::kFunctionSignature);
  auto arguments = Extract<std::vector<NodePtr>>(document, field::kArguments);

  return std::make_shared<FunctionNode>(std::move(funciton_signature), std::move(arguments));
}

template <>
std::shared_ptr<ScalarOverArrayFunctionNode> Deserialize(const rapidjson::Value& document) {
  Ensure(document.IsObject(), "Deserialize (FunctionArrayNode): wrong type");

  FunctionSignature funciton_signature = Extract<FunctionSignature>(document, field::kFunctionSignature);
  bool use_or = Extract<bool>(document, field::kUseOr);
  NodePtr lhs = Extract<NodePtr>(document, field::kLhs);
  ArrayHolder rhs = Extract<ArrayHolder>(document, field::kRhs);

  return std::make_shared<ScalarOverArrayFunctionNode>(std::move(funciton_signature), use_or, lhs, rhs);
}

template <>
std::shared_ptr<IfNode> Deserialize(const rapidjson::Value& document) {
  Ensure(document.IsObject(), "Deserialize (IfNode): wrong type");

  NodePtr condition = Extract<NodePtr>(document, field::kCondition);
  NodePtr then_node = Extract<NodePtr>(document, field::kThen);
  NodePtr else_node = Extract<NodePtr>(document, field::kElse);

  return std::make_shared<IfNode>(condition, then_node, else_node);
}

template <>
std::shared_ptr<VariableNode> Deserialize(const rapidjson::Value& document) {
  Ensure(document.IsObject(), "Deserialize (VariableNode): wrong type");

  ValueType value_type = StringToValueType(Extract<std::string>(document, field::kValueType));
  std::string column_name = Extract<std::string>(document, field::kColumnName);

  return std::make_shared<VariableNode>(value_type, std::move(column_name));
}

template <>
NodePtr Deserialize(const rapidjson::Value& document) {
  NodeType node_type = StringToNodeType(Extract<std::string>(document, field::kNodeType));
  switch (node_type) {
    case NodeType::kConst:
      return Deserialize<std::shared_ptr<ConstNode>>(document);
    case NodeType::kScalarOverArrayFunction:
      return Deserialize<std::shared_ptr<ScalarOverArrayFunctionNode>>(document);
    case NodeType::kVariable:
      return Deserialize<std::shared_ptr<VariableNode>>(document);
    case NodeType::kIf:
      return Deserialize<std::shared_ptr<IfNode>>(document);
    case NodeType::kFunction:
      return Deserialize<std::shared_ptr<FunctionNode>>(document);
    case NodeType::kLogical:
      return Deserialize<std::shared_ptr<LogicalNode>>(document);
  }
  throw std::runtime_error(__PRETTY_FUNCTION__ + std::string(": internal error"));
}

}  // namespace serializer

NodePtr StringToFilter(const std::string& row_filter) {
  if (row_filter.empty()) {
    return nullptr;
  }

  rapidjson::Document document;
  document.Parse(row_filter.c_str(), row_filter.length());

  return serializer::Deserialize<NodePtr>(document);
}

std::string FilterToString(NodePtr node) {
  if (!node) {
    return "";
  }

  serializer::Serializer serializer;
  rapidjson::Value value = serializer.Serialize(node);
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  value.Accept(writer);

  return buffer.GetString();
}

}  // namespace iceberg::filter
