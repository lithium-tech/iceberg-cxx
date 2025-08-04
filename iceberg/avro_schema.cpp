#include "iceberg/avro_schema.h"

#include <stdexcept>

#include "arrow/type.h"
#include "iceberg/streams/arrow/error.h"
#include "rapidjson/document.h"
#include "rapidjson/prettywriter.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"

namespace iceavro {

namespace {

int MinimumBytesToStoreDecimal(int precision) {
  const int kMaxPrecision = 38;
  if (precision > kMaxPrecision) {
    throw std::runtime_error("MinimumBytesToStoreDecimal: precision is greater than " + std::to_string(kMaxPrecision) +
                             " is not supported");
  }
  if (precision <= 0) {
    throw std::runtime_error("MinimumBytesToStoreDecimal: precision is less than or equal to " + std::to_string(0) +
                             " is not supported");
  }
  return arrow::DecimalType::DecimalSize(precision);
}

class Serializer {
 public:
  using Allocator = rapidjson::MemoryPoolAllocator<rapidjson::CrtAllocator>;

  rapidjson::Value Serialize(std::shared_ptr<BoolNode> node) {
    rapidjson::Value result(rapidjson::kObjectType);

    AddMember(result, "type", "boolean");
    return result;
  }

  rapidjson::Value Serialize(std::shared_ptr<IntNode> node) {
    rapidjson::Value result(rapidjson::kObjectType);

    AddMember(result, "type", "int");
    return result;
  }

  rapidjson::Value Serialize(std::shared_ptr<FloatNode> node) {
    rapidjson::Value result(rapidjson::kObjectType);

    AddMember(result, "type", "float");
    return result;
  }

  rapidjson::Value Serialize(std::shared_ptr<DoubleNode> node) {
    rapidjson::Value result(rapidjson::kObjectType);

    AddMember(result, "type", "double");
    return result;
  }

  rapidjson::Value Serialize(std::shared_ptr<DateNode> node) {
    rapidjson::Value result(rapidjson::kObjectType);

    {
      rapidjson::Value true_type(rapidjson::kObjectType);

      AddMember(true_type, "type", "int");
      AddMember(true_type, "logicalType", "date");

      result.AddMember("type", std::move(true_type), allocator_);
    }

    return result;
  }

  rapidjson::Value Serialize(std::shared_ptr<FixedNode> node) {
    rapidjson::Value result(rapidjson::kObjectType);

    {
      rapidjson::Value true_type(rapidjson::kObjectType);

      AddMember(true_type, "type", "fixed");
      AddMember(true_type, "size", node->size_);

      result.AddMember("type", std::move(true_type), allocator_);
    }

    return result;
  }

  rapidjson::Value Serialize(std::shared_ptr<UuidNode> node) {
    rapidjson::Value result(rapidjson::kObjectType);

    {
      rapidjson::Value true_type(rapidjson::kObjectType);

      AddMember(true_type, "type", "fixed");
      AddMember(true_type, "name", "uuid_fixed");
      AddMember(true_type, "size", 16);
      AddMember(true_type, "logicalType", "uuid");

      result.AddMember("type", std::move(true_type), allocator_);
    }

    return result;
  }

  rapidjson::Value Serialize(std::shared_ptr<TimeNode> node) {
    rapidjson::Value result(rapidjson::kObjectType);

    {
      rapidjson::Value true_type(rapidjson::kObjectType);

      AddMember(true_type, "type", "long");
      AddMember(true_type, "logicalType", "time-micros");

      result.AddMember("type", std::move(true_type), allocator_);
    }

    return result;
  }

  rapidjson::Value Serialize(std::shared_ptr<TimestampNode> node) {
    rapidjson::Value result(rapidjson::kObjectType);

    {
      rapidjson::Value true_type(rapidjson::kObjectType);

      AddMember(true_type, "type", "long");
      AddMember(true_type, "logicalType", "timestamp-micros");
      AddMember(true_type, "adjust-to-utc", false);

      result.AddMember("type", std::move(true_type), allocator_);
    }

    return result;
  }

  rapidjson::Value Serialize(std::shared_ptr<TimestamptzNode> node) {
    rapidjson::Value result(rapidjson::kObjectType);

    {
      rapidjson::Value true_type(rapidjson::kObjectType);

      AddMember(true_type, "type", "long");
      AddMember(true_type, "logicalType", "timestamp-micros");
      AddMember(true_type, "adjust-to-utc", true);

      result.AddMember("type", std::move(true_type), allocator_);
    }

    return result;
  }

  rapidjson::Value Serialize(std::shared_ptr<TimestampNsNode> node) {
    rapidjson::Value result(rapidjson::kObjectType);

    {
      rapidjson::Value true_type(rapidjson::kObjectType);

      AddMember(true_type, "type", "long");
      AddMember(true_type, "logicalType", "timestamp-nanos");
      AddMember(true_type, "adjust-to-utc", false);

      result.AddMember("type", std::move(true_type), allocator_);
    }

    return result;
  }

  rapidjson::Value Serialize(std::shared_ptr<TimestamptzNsNode> node) {
    rapidjson::Value result(rapidjson::kObjectType);

    {
      rapidjson::Value true_type(rapidjson::kObjectType);

      AddMember(true_type, "type", "long");
      AddMember(true_type, "logicalType", "timestamp-nanos");
      AddMember(true_type, "adjust-to-utc", true);

      result.AddMember("type", std::move(true_type), allocator_);
    }

    return result;
  }

  rapidjson::Value Serialize(std::shared_ptr<DecimalNode> node) {
    rapidjson::Value result(rapidjson::kObjectType);

    {
      rapidjson::Value true_type(rapidjson::kObjectType);

      AddMember(true_type, "type", "fixed");
      AddMember(true_type, "size", MinimumBytesToStoreDecimal(node->precision_));
      AddMember(true_type, "name", "decimal_" + std::to_string(node->precision_) + "_" + std::to_string(node->scale_));
      AddMember(true_type, "logicalType", "decimal");
      AddMember(true_type, "precision", node->precision_);
      AddMember(true_type, "scale", node->scale_);

      result.AddMember("type", std::move(true_type), allocator_);
    }

    return result;
  }

  rapidjson::Value Serialize(std::shared_ptr<LongNode> node) {
    rapidjson::Value result(rapidjson::kObjectType);

    AddMember(result, "type", "long");
    return result;
  }

  rapidjson::Value Serialize(std::shared_ptr<StringNode> node) {
    rapidjson::Value result(rapidjson::kObjectType);

    AddMember(result, "type", "string");
    return result;
  }

  rapidjson::Value Serialize(std::shared_ptr<BytesNode> node) {
    rapidjson::Value result(rapidjson::kObjectType);

    AddMember(result, "type", "bytes");
    return result;
  }

  rapidjson::Value Serialize(std::vector<std::pair<std::string, std::shared_ptr<Node>>> fields) {
    rapidjson::Value result(rapidjson::kArrayType);

    for (auto& [name, field] : fields) {
      rapidjson::Value field_value = Serialize(field);
      iceberg::Ensure(field_value.IsObject(),
                      std::string(__PRETTY_FUNCTION__) + ": internal error. Field must be an object");

      AddMember(field_value, "name", name);
      result.PushBack(field_value, allocator_);
    }

    return result;
  }

  rapidjson::Value Serialize(std::shared_ptr<RecordNode> node) {
    rapidjson::Value result(rapidjson::kObjectType);

    {
      rapidjson::Value type_result(rapidjson::kObjectType);

      AddMember(type_result, "type", "record");
      AddMember(type_result, "name", node->name_);
      AddMember(type_result, "fields", node->fields_);

      result.AddMember("type", std::move(type_result), allocator_);
    }
    return result;
  }

  rapidjson::Value Serialize(std::shared_ptr<OptionalNode> node) {
    rapidjson::Value result = Serialize(node->child_);
    iceberg::Ensure(result.IsObject(),
                    std::string(__PRETTY_FUNCTION__) + ": internal error. OptionalNode child must be an object");

    rapidjson::GenericObject object = result.GetObject();
    iceberg::Ensure(object.HasMember("type"),
                    std::string(__PRETTY_FUNCTION__) + ": internal error. OptionalNode child must has field 'type'");

    rapidjson::Value possible_types(rapidjson::kArrayType);
    possible_types.PushBack("null", allocator_);
    possible_types.PushBack(object.FindMember("type")->value, allocator_);

    result.EraseMember("type");

    result.AddMember("type", std::move(possible_types), allocator_);
    result.AddMember("default", rapidjson::Value(), allocator_);
    return result;
  }

  rapidjson::Value Serialize(std::shared_ptr<ArrayNode> node) {
    rapidjson::Value element = Serialize(node->element_);
    iceberg::Ensure(element.IsObject(),
                    std::string(__PRETTY_FUNCTION__) + ": internal error. ArrayNode element must be an object");

    rapidjson::GenericObject object = element.GetObject();
    iceberg::Ensure(object.HasMember("type"),
                    std::string(__PRETTY_FUNCTION__) + ": internal error. ArrayNode element must has field 'type'");

    rapidjson::Value type_result(rapidjson::kObjectType);
    type_result.AddMember("items", object.FindMember("type")->value, allocator_);
    type_result.AddMember("type", "array", allocator_);

    rapidjson::Value result(rapidjson::kObjectType);
    result.AddMember("type", std::move(type_result), allocator_);
    return result;
  }

  rapidjson::Value Serialize(std::same_as<std::shared_ptr<Node>> auto node) {
    switch (node->node_type_) {
      case NodeType::kBool:
        return Serialize(std::static_pointer_cast<BoolNode>(node));
      case NodeType::kInt:
        return Serialize(std::static_pointer_cast<IntNode>(node));
      case NodeType::kLong:
        return Serialize(std::static_pointer_cast<LongNode>(node));
      case NodeType::kFloat:
        return Serialize(std::static_pointer_cast<FloatNode>(node));
      case NodeType::kDouble:
        return Serialize(std::static_pointer_cast<DoubleNode>(node));
      case NodeType::kTime:
        return Serialize(std::static_pointer_cast<TimeNode>(node));
      case NodeType::kTimestamp:
        return Serialize(std::static_pointer_cast<TimestampNode>(node));
      case NodeType::kTimestamptz:
        return Serialize(std::static_pointer_cast<TimestamptzNode>(node));
      case NodeType::kTimestampNs:
        return Serialize(std::static_pointer_cast<TimestampNsNode>(node));
      case NodeType::kTimestamptzNs:
        return Serialize(std::static_pointer_cast<TimestamptzNsNode>(node));
      case NodeType::kDate:
        return Serialize(std::static_pointer_cast<DateNode>(node));
      case NodeType::kString:
        return Serialize(std::static_pointer_cast<StringNode>(node));
      case NodeType::kBytes:
        return Serialize(std::static_pointer_cast<BytesNode>(node));
      case NodeType::kDecimal:
        return Serialize(std::static_pointer_cast<DecimalNode>(node));
      case NodeType::kFixed:
        return Serialize(std::static_pointer_cast<FixedNode>(node));
      case NodeType::kUuid:
        return Serialize(std::static_pointer_cast<UuidNode>(node));
      case NodeType::kOptional:
        return Serialize(std::static_pointer_cast<OptionalNode>(node));
      case NodeType::kArray:
        return Serialize(std::static_pointer_cast<ArrayNode>(node));
      case NodeType::kRecord:
        return Serialize(std::static_pointer_cast<RecordNode>(node));
      default:
        throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": node type " +
                                 std::to_string(static_cast<int>(node->node_type_)) + " is not supported");
    }
  }

  rapidjson::Value Serialize(const std::string& str) {
    rapidjson::Value result(str.c_str(), allocator_);
    return result;
  }

  rapidjson::Value Serialize(std::same_as<int64_t> auto value) { return rapidjson::Value(value); }

  rapidjson::Value Serialize(std::same_as<int32_t> auto value) { return rapidjson::Value(value); }

  rapidjson::Value Serialize(std::same_as<bool> auto value) { return rapidjson::Value(value); }

  template <typename T>
  void AddMember(rapidjson::Value& result, std::string_view name, T&& value) {
    result.AddMember(rapidjson::Value(name.data(), allocator_), Serialize(std::forward<T>(value)), allocator_);
  }

 private:
  Allocator allocator_;
};

}  // namespace

std::string ToJsonString(std::shared_ptr<Node> node) {
  Serializer serializer;
  rapidjson::Value json_node = serializer.Serialize(node);
  iceberg::Ensure(json_node.IsObject(), std::string(__PRETTY_FUNCTION__) + ": internal error. Node must be an object");
  iceberg::Ensure(json_node.HasMember("type"),
                  std::string(__PRETTY_FUNCTION__) + ": internal error. Node must has field 'type'");

  rapidjson::Value& val = json_node.FindMember("type")->value;
  rapidjson::StringBuffer buffer;
  rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(buffer);
  val.Accept(writer);
  return std::string(buffer.GetString(), buffer.GetLength());
}

}  // namespace iceavro
