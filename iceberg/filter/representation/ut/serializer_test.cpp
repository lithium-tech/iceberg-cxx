#include "iceberg/filter/representation/serializer.h"

#include "gtest/gtest.h"
#include "iceberg/filter/representation/function.h"
#include "iceberg/filter/representation/node.h"
#include "iceberg/filter/representation/value.h"

namespace iceberg::filter {

namespace {

using iceberg::filter::ValueType::kBool;
using iceberg::filter::ValueType::kDate;
using iceberg::filter::ValueType::kFloat4;
using iceberg::filter::ValueType::kFloat8;
using iceberg::filter::ValueType::kInt2;
using iceberg::filter::ValueType::kInt4;
using iceberg::filter::ValueType::kInt8;
using iceberg::filter::ValueType::kInterval;
using iceberg::filter::ValueType::kNumeric;
using iceberg::filter::ValueType::kString;
using iceberg::filter::ValueType::kTime;
using iceberg::filter::ValueType::kTimestamp;
using iceberg::filter::ValueType::kTimestamptz;

void DeserializeSerializeCheck(const std::string& filter) {
  auto deserialized_filter = iceberg::filter::StringToFilter(filter);
  auto serialized_filter = iceberg::filter::FilterToString(deserialized_filter);

  EXPECT_EQ(filter, serialized_filter);
}

TEST(SerializerTest, Variable) {
  auto node = std::make_shared<filter::VariableNode>(kBool, "col1");
  auto serialized_filter = iceberg::filter::FilterToString(node);
  ASSERT_EQ(serialized_filter, "{\"node-type\":\"var\",\"val-type\":\"bool\",\"col-name\":\"col1\"}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, ConstBool) {
  auto node = std::make_shared<filter::ConstNode>(Value::Make<kBool>(false));
  auto serialized_filter = iceberg::filter::FilterToString(node);
  ASSERT_EQ(serialized_filter, "{\"node-type\":\"const\",\"value\":{\"val-type\":\"bool\",\"value\":false}}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, ConstInt2) {
  auto node = std::make_shared<filter::ConstNode>(Value::Make<kInt2>(12));
  auto serialized_filter = iceberg::filter::FilterToString(node);
  ASSERT_EQ(serialized_filter, "{\"node-type\":\"const\",\"value\":{\"val-type\":\"int2\",\"value\":12}}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, ConstInt4) {
  auto node = std::make_shared<filter::ConstNode>(Value::Make<kInt4>(12));
  auto serialized_filter = iceberg::filter::FilterToString(node);
  ASSERT_EQ(serialized_filter, "{\"node-type\":\"const\",\"value\":{\"val-type\":\"int4\",\"value\":12}}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, ConstInt8) {
  auto node = std::make_shared<filter::ConstNode>(Value::Make<kInt8>(12));
  auto serialized_filter = iceberg::filter::FilterToString(node);
  ASSERT_EQ(serialized_filter, "{\"node-type\":\"const\",\"value\":{\"val-type\":\"int8\",\"value\":12}}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, ConstFloat4) {
  auto node = std::make_shared<filter::ConstNode>(Value::Make<kFloat4>(12.0));
  auto serialized_filter = iceberg::filter::FilterToString(node);
  ASSERT_EQ(serialized_filter, "{\"node-type\":\"const\",\"value\":{\"val-type\":\"float4\",\"value\":12.0}}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, ConstFloat8) {
  auto node = std::make_shared<filter::ConstNode>(Value::Make<kFloat8>(12.0));
  auto serialized_filter = iceberg::filter::FilterToString(node);
  ASSERT_EQ(serialized_filter, "{\"node-type\":\"const\",\"value\":{\"val-type\":\"float8\",\"value\":12.0}}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, ConstNumeric) {
  auto node = std::make_shared<filter::ConstNode>(Value::Make<kNumeric>(Numeric{"12.34"}));
  auto serialized_filter = iceberg::filter::FilterToString(node);
  ASSERT_EQ(serialized_filter, "{\"node-type\":\"const\",\"value\":{\"val-type\":\"numeric\",\"value\":\"12.34\"}}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, ConstString) {
  auto node = std::make_shared<filter::ConstNode>(Value::Make<kString>("abacaba42"));
  auto serialized_filter = iceberg::filter::FilterToString(node);
  ASSERT_EQ(serialized_filter, "{\"node-type\":\"const\",\"value\":{\"val-type\":\"string\",\"value\":\"abacaba42\"}}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, ConstTime) {
  auto node = std::make_shared<filter::ConstNode>(Value::Make<kTime>(42));
  auto serialized_filter = iceberg::filter::FilterToString(node);
  ASSERT_EQ(serialized_filter, "{\"node-type\":\"const\",\"value\":{\"val-type\":\"time\",\"value\":42}}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, ConstTimestamp) {
  auto node = std::make_shared<filter::ConstNode>(Value::Make<kTimestamp>(42));
  auto serialized_filter = iceberg::filter::FilterToString(node);
  ASSERT_EQ(serialized_filter, "{\"node-type\":\"const\",\"value\":{\"val-type\":\"timestamp\",\"value\":42}}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, ConstTimestamptz) {
  auto node = std::make_shared<filter::ConstNode>(Value::Make<kTimestamptz>(42));
  auto serialized_filter = iceberg::filter::FilterToString(node);
  ASSERT_EQ(serialized_filter, "{\"node-type\":\"const\",\"value\":{\"val-type\":\"timestamptz\",\"value\":42}}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, ConstDate) {
  auto node = std::make_shared<filter::ConstNode>(Value::Make<kDate>(42));
  auto serialized_filter = iceberg::filter::FilterToString(node);
  ASSERT_EQ(serialized_filter, "{\"node-type\":\"const\",\"value\":{\"val-type\":\"date\",\"value\":42}}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, ConstInterval) {
  auto node =
      std::make_shared<filter::ConstNode>(Value::Make<kInterval>(IntervalMonthMicro{.months = 1, .micros = 23}));
  auto serialized_filter = iceberg::filter::FilterToString(node);
  ASSERT_EQ(serialized_filter,
            "{\"node-type\":\"const\",\"value\":{\"val-type\":\"interval\",\"value\":{\"months\":1,\"micros\":23}}}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, ConstNull) {
  auto node = std::make_shared<filter::ConstNode>(Value::Make<kInt4>());
  auto serialized_filter = iceberg::filter::FilterToString(node);
  ASSERT_EQ(serialized_filter, "{\"node-type\":\"const\",\"value\":{\"val-type\":\"int4\",\"value\":null}}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, FunctionNode) {
  auto lhs_node = std::make_shared<filter::VariableNode>(kInt4, "col1");
  auto rhs_node = std::make_shared<filter::VariableNode>(kInt4, "col2");

  auto root_node =
      std::make_shared<filter::FunctionNode>(FunctionSignature{.function_id = FunctionID::kLessThan,
                                                               .return_type = kBool,
                                                               .argument_types = std::vector<ValueType>{kInt4, kInt4}},
                                             std::vector<NodePtr>{lhs_node, rhs_node});

  auto serialized_filter = iceberg::filter::FilterToString(root_node);
  ASSERT_EQ(serialized_filter,
            "{\"node-type\":\"func\",\"func-sig\":{\"arg-types\":[\"int4\",\"int4\"],\"val-type\":\"bool\",\"func-id\":"
            "\"kLessThan\"},\"args\":[{\"node-type\":\"var\",\"val-type\":\"int4\",\"col-name\":\"col1\"},{\"node-"
            "type\":\"var\",\"val-type\":\"int4\",\"col-name\":\"col2\"}]}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, IfNode) {
  auto condition = std::make_shared<filter::VariableNode>(kBool, "col1");
  auto then_node = std::make_shared<filter::ConstNode>(Value::Make<kInt2>(42));
  auto else_node = std::make_shared<filter::ConstNode>(Value::Make<kInt4>(2101));

  auto root_node = std::make_shared<filter::IfNode>(condition, then_node, else_node);

  auto serialized_filter = iceberg::filter::FilterToString(root_node);
  ASSERT_EQ(serialized_filter,
            "{\"node-type\":\"if\",\"cond\":{\"node-type\":\"var\",\"val-type\":\"bool\",\"col-name\":\"col1\"},"
            "\"then\":{\"node-type\":\"const\",\"value\":{\"val-type\":\"int2\",\"value\":42}},\"else\":{\"node-type\":"
            "\"const\",\"value\":{\"val-type\":\"int4\",\"value\":2101}}}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, FunctionArrayBool) {
  auto lhs_node = std::make_shared<filter::VariableNode>(kBool, "col1");
  auto array = ArrayHolder::Make<kBool>(Array<kBool>{false, std::nullopt, true});
  auto func_array_node = std::make_shared<filter::ScalarOverArrayFunctionNode>(
      FunctionSignature{.function_id = FunctionID::kEqual,
                        .return_type = kBool,
                        .argument_types = std::vector<ValueType>{kBool, kBool}},
      true, lhs_node, std::move(array));

  auto serialized_filter = iceberg::filter::FilterToString(func_array_node);
  ASSERT_EQ(serialized_filter,
            "{\"node-type\":\"funcarr\",\"func-sig\":{\"arg-types\":[\"bool\",\"bool\"],\"val-type\":\"bool\",\"func-"
            "id\":\"kEqual\"},\"use-or\":true,\"lhs\":{\"node-type\":\"var\",\"val-type\":\"bool\",\"col-name\":"
            "\"col1\"},\"rhs\":{\"val-type\":\"bool\",\"value\":[false,null,true]}}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, FunctionArrayInt4) {
  auto lhs_node = std::make_shared<filter::VariableNode>(kInt4, "col1");
  auto array = ArrayHolder::Make<kInt4>(Array<kInt4>{-1, std::nullopt, 42});
  auto func_array_node = std::make_shared<filter::ScalarOverArrayFunctionNode>(
      FunctionSignature{.function_id = FunctionID::kEqual,
                        .return_type = kBool,
                        .argument_types = std::vector<ValueType>{kBool, kBool}},
      true, lhs_node, std::move(array));

  auto serialized_filter = iceberg::filter::FilterToString(func_array_node);
  ASSERT_EQ(serialized_filter,
            "{\"node-type\":\"funcarr\",\"func-sig\":{\"arg-types\":[\"bool\",\"bool\"],\"val-type\":\"bool\",\"func-"
            "id\":\"kEqual\"},\"use-or\":true,\"lhs\":{\"node-type\":\"var\",\"val-type\":\"int4\",\"col-name\":"
            "\"col1\"},\"rhs\":{\"val-type\":\"int4\",\"value\":[-1,null,42]}}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, FunctionArrayFloat8) {
  auto lhs_node = std::make_shared<filter::VariableNode>(kFloat8, "col1");
  auto array = ArrayHolder::Make<kFloat8>(Array<kFloat8>{-1, std::nullopt, 4.25});
  auto func_array_node = std::make_shared<filter::ScalarOverArrayFunctionNode>(
      FunctionSignature{.function_id = FunctionID::kEqual,
                        .return_type = kBool,
                        .argument_types = std::vector<ValueType>{kFloat8, kFloat8}},
      true, lhs_node, std::move(array));

  auto serialized_filter = iceberg::filter::FilterToString(func_array_node);
  ASSERT_EQ(serialized_filter,
            "{\"node-type\":\"funcarr\",\"func-sig\":{\"arg-types\":[\"float8\",\"float8\"],\"val-type\":\"bool\","
            "\"func-id\":\"kEqual\"},\"use-or\":true,\"lhs\":{\"node-type\":\"var\",\"val-type\":\"float8\",\"col-"
            "name\":\"col1\"},\"rhs\":{\"val-type\":\"float8\",\"value\":[-1.0,null,4.25]}}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, FunctionArrayNumeric) {
  auto lhs_node = std::make_shared<filter::VariableNode>(kNumeric, "col1");
  auto array = ArrayHolder::Make<kNumeric>(Array<kNumeric>{Numeric{"0"}, std::nullopt, Numeric{"4.2"}});
  auto func_array_node = std::make_shared<filter::ScalarOverArrayFunctionNode>(
      FunctionSignature{.function_id = FunctionID::kEqual,
                        .return_type = kBool,
                        .argument_types = std::vector<ValueType>{kNumeric, kNumeric}},
      true, lhs_node, std::move(array));

  auto serialized_filter = iceberg::filter::FilterToString(func_array_node);
  ASSERT_EQ(serialized_filter,
            "{\"node-type\":\"funcarr\",\"func-sig\":{\"arg-types\":[\"numeric\",\"numeric\"],\"val-type\":\"bool\","
            "\"func-id\":\"kEqual\"},\"use-or\":true,\"lhs\":{\"node-type\":\"var\",\"val-type\":\"numeric\",\"col-"
            "name\":\"col1\"},\"rhs\":{\"val-type\":\"numeric\",\"value\":[\"0\",null,\"4.2\"]}}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, FunctionArrayString) {
  auto lhs_node = std::make_shared<filter::VariableNode>(kString, "col1");
  auto array = ArrayHolder::Make<kString>(Array<kString>{"", std::nullopt, "a.2"});
  auto func_array_node = std::make_shared<filter::ScalarOverArrayFunctionNode>(
      FunctionSignature{.function_id = FunctionID::kEqual,
                        .return_type = kBool,
                        .argument_types = std::vector<ValueType>{kString, kString}},
      true, lhs_node, std::move(array));

  auto serialized_filter = iceberg::filter::FilterToString(func_array_node);
  ASSERT_EQ(serialized_filter,
            "{\"node-type\":\"funcarr\",\"func-sig\":{\"arg-types\":[\"string\",\"string\"],\"val-type\":\"bool\","
            "\"func-id\":\"kEqual\"},\"use-or\":true,\"lhs\":{\"node-type\":\"var\",\"val-type\":\"string\",\"col-"
            "name\":\"col1\"},\"rhs\":{\"val-type\":\"string\",\"value\":[\"\",null,\"a.2\"]}}");

  DeserializeSerializeCheck(serialized_filter);
}

TEST(SerializerTest, LogicalNode) {
  auto lhs_node = std::make_shared<filter::VariableNode>(kBool, "col1");
  auto rhs_node = std::make_shared<filter::VariableNode>(kBool, "col2");

  auto root_node = std::make_shared<filter::LogicalNode>(filter::LogicalNode::Operation::kAnd,
                                                         std::vector<NodePtr>{lhs_node, rhs_node});

  auto serialized_filter = iceberg::filter::FilterToString(root_node);
  ASSERT_EQ(serialized_filter,
            "{\"node-type\":\"logical\",\"logic-op\":\"kAnd\",\"args\":[{\"node-type\":\"var\",\"val-type\":\"bool\","
            "\"col-name\":\"col1\"},{\"node-type\":\"var\",\"val-type\":\"bool\",\"col-name\":\"col2\"}]}");

  DeserializeSerializeCheck(serialized_filter);
}

}  // namespace

}  // namespace iceberg::filter
