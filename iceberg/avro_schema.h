#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace iceavro {

enum class NodeType {
  kBool,
  kInt,
  kLong,
  kDate,
  kTime,
  kTimestamp,
  kTimestamptz,
  kTimestampNs,
  kTimestamptzNs,
  kString,
  kFloat,
  kDouble,
  kBytes,
  kDecimal,
  kFixed,
  kUuid,
  kArray,
  kRecord,
  kOptional
};

// TODO(gmusya): use field id
struct Node {
  NodeType node_type_;
  std::optional<int32_t> field_id_;

  Node(NodeType node_type, std::optional<int32_t> field_id = std::nullopt)
      : node_type_(node_type), field_id_(std::move(field_id)) {}
};

struct BoolNode : public Node {
  explicit BoolNode(std::optional<int32_t> field_id = std::nullopt) : Node(NodeType::kBool, field_id) {}
};

struct IntNode : public Node {
  explicit IntNode(std::optional<int32_t> field_id = std::nullopt) : Node(NodeType::kInt, field_id) {}
};

struct LongNode : public Node {
  explicit LongNode(std::optional<int32_t> field_id = std::nullopt) : Node(NodeType::kLong, field_id) {}
};

struct FloatNode : public Node {
  explicit FloatNode(std::optional<int32_t> field_id = std::nullopt) : Node(NodeType::kFloat, field_id) {}
};

struct DoubleNode : public Node {
  explicit DoubleNode(std::optional<int32_t> field_id = std::nullopt) : Node(NodeType::kDouble, field_id) {}
};

struct DateNode : public Node {
  explicit DateNode(std::optional<int32_t> field_id = std::nullopt) : Node(NodeType::kDate, field_id) {}
};

struct TimeNode : public Node {
  explicit TimeNode(std::optional<int32_t> field_id = std::nullopt) : Node(NodeType::kTime, field_id) {}
};

struct TimestampNode : public Node {
  explicit TimestampNode(std::optional<int32_t> field_id = std::nullopt) : Node(NodeType::kTimestamp, field_id) {}
};

struct TimestamptzNode : public Node {
  explicit TimestamptzNode(std::optional<int32_t> field_id = std::nullopt) : Node(NodeType::kTimestamptz, field_id) {}
};

struct TimestampNsNode : public Node {
  explicit TimestampNsNode(std::optional<int32_t> field_id = std::nullopt) : Node(NodeType::kTimestampNs, field_id) {}
};

struct TimestamptzNsNode : public Node {
  explicit TimestamptzNsNode(std::optional<int32_t> field_id = std::nullopt)
      : Node(NodeType::kTimestamptzNs, field_id) {}
};

struct DecimalNode : public Node {
  explicit DecimalNode(int32_t precision, int32_t scale, std::optional<int32_t> field_id = std::nullopt)
      : Node(NodeType::kDecimal, field_id), precision_(precision), scale_(scale) {}

  int32_t precision_;
  int32_t scale_;
};

struct StringNode : public Node {
  explicit StringNode(std::optional<int32_t> field_id = std::nullopt) : Node(NodeType::kString, field_id) {}
};

struct BytesNode : public Node {
  explicit BytesNode(std::optional<int32_t> field_id = std::nullopt) : Node(NodeType::kBytes, field_id) {}
};

struct FixedNode : public Node {
  explicit FixedNode(int32_t size, std::optional<int32_t> field_id = std::nullopt)
      : Node(NodeType::kFixed, field_id), size_(size) {}

  int32_t size_;
};

struct UuidNode : public Node {
  explicit UuidNode(std::optional<int32_t> field_id = std::nullopt) : Node(NodeType::kUuid, field_id) {}
};

struct OptionalNode : public Node {
  explicit OptionalNode(std::shared_ptr<Node> child, std::optional<int32_t> field_id = std::nullopt)
      : Node(NodeType::kOptional, field_id), child_(child) {}

  std::shared_ptr<Node> child_;
};

struct ArrayNode : public Node {
  explicit ArrayNode(std::shared_ptr<Node> element, std::optional<int32_t> field_id = std::nullopt,
                     std::optional<int32_t> element_id = std::nullopt)
      : Node(NodeType::kArray, field_id), element_(element), element_id_(element_id) {}

  std::shared_ptr<Node> element_;
  std::optional<int32_t> element_id_;
};

struct RecordNode : public Node {
  explicit RecordNode(std::string name, std::optional<int32_t> field_id = std::nullopt)
      : Node(NodeType::kRecord, field_id), name_(std::move(name)) {}

  void AddField(const std::string& name, std::shared_ptr<Node> field) { fields_.emplace_back(name, field); }

  std::string name_;
  std::vector<std::pair<std::string, std::shared_ptr<Node>>> fields_;
};

std::string ToJsonString(std::shared_ptr<Node>);

}  // namespace iceavro
