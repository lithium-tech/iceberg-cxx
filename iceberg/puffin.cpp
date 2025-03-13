#include "iceberg/puffin.h"

#include <stdexcept>

#include "arrow/status.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

namespace iceberg {
namespace {

namespace serializer {

namespace field {
constexpr std::string_view kType = "type";
constexpr std::string_view kFields = "fields";
constexpr std::string_view kSnapshotId = "snapshot-id";
constexpr std::string_view kSequenceNumber = "sequence-number";
constexpr std::string_view kOffset = "offset";
constexpr std::string_view kLength = "length";
constexpr std::string_view kCompressionCodec = "compression-codec";
constexpr std::string_view kProperties = "properties";
constexpr std::string_view kBlobs = "blobs";
}  // namespace field

using Allocator = rapidjson::MemoryPoolAllocator<rapidjson::CrtAllocator>;

class Serializer {
 public:
  rapidjson::Value Serialize(const PuffinFile::Footer::DeserializedFooter&);

 private:
  // clang-format off
  template <typename T>
  rapidjson::Value Serialize(const T&)
  requires(std::is_same_v<std::vector<typename T::value_type>, T>);

  template <typename T>
  rapidjson::Value Serialize(const T&)
  requires(std::is_same_v<std::optional<typename T::value_type>, T>);
  // clang-format on

  rapidjson::Value Serialize(const PuffinFile::Footer::BlobMetadata&);
  rapidjson::Value Serialize(const std::map<std::string, std::string>&);

  rapidjson::Value Serialize(std::same_as<int32_t> auto);
  rapidjson::Value Serialize(std::same_as<int64_t> auto);
  rapidjson::Value Serialize(std::string_view);
  rapidjson::Value Serialize(const std::string&);

 private:
  void AddMember(rapidjson::Value& result, std::string_view key, rapidjson::Value&& value);

  Allocator allocator_;
};

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

void Serializer::AddMember(rapidjson::Value& result, std::string_view key, rapidjson::Value&& value) {
  result.AddMember(rapidjson::Value(key.data(), allocator_), std::move(value), allocator_);
}

rapidjson::Value Serializer::Serialize(std::same_as<int64_t> auto value) { return rapidjson::Value(value); }
rapidjson::Value Serializer::Serialize(std::same_as<int32_t> auto value) { return rapidjson::Value(value); }
rapidjson::Value Serializer::Serialize(std::string_view sv) { return rapidjson::Value(sv.data(), allocator_); }
rapidjson::Value Serializer::Serialize(const std::string& sv) { return rapidjson::Value(sv.data(), allocator_); }

rapidjson::Value Serializer::Serialize(const PuffinFile::Footer::BlobMetadata& blob) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddMember(result, field::kFields, Serialize(blob.fields));
  AddMember(result, field::kType, Serialize(blob.type));
  AddMember(result, field::kCompressionCodec, Serialize(blob.compression_codec));
  AddMember(result, field::kLength, Serialize(blob.length));
  AddMember(result, field::kOffset, Serialize(blob.offset));
  AddMember(result, field::kProperties, Serialize(blob.properties));
  AddMember(result, field::kSequenceNumber, Serialize(blob.sequence_number));
  AddMember(result, field::kSnapshotId, Serialize(blob.snapshot_id));
  return result;
}

rapidjson::Value Serializer::Serialize(const std::map<std::string, std::string>& props) {
  rapidjson::Value result(rapidjson::kObjectType);
  for (const auto& [key, value] : props) {
    AddMember(result, key, Serialize(value));
  }
  return result;
}

rapidjson::Value Serializer::Serialize(const PuffinFile::Footer::DeserializedFooter& footer) {
  rapidjson::Value result(rapidjson::kObjectType);
  AddMember(result, field::kBlobs, Serialize(footer.blobs));
  AddMember(result, field::kProperties, Serialize(footer.properties));
  return result;
}

void Ensure(bool condition, const std::string& error_message) {
  if (!condition) {
    throw std::runtime_error(error_message);
  }
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

template <typename T>
T Extract(const rapidjson::Value& document, std::string_view field_name) {
  const char* c_str = field_name.data();
  Ensure(document.HasMember(c_str), "ExtractPrimitive: !document.HasMember(" + std::string(field_name) + ")");
  return Deserialize<T>(document[c_str]);
}

template <>
std::map<std::string, std::string> Deserialize(const rapidjson::Value& document) {
  Ensure(document.IsObject(), std::string(__PRETTY_FUNCTION__));
  std::map<std::string, std::string> result;
  const auto member_begin = document.MemberBegin();
  const auto member_end = document.MemberEnd();

  for (auto member = member_begin; member != member_end; ++member) {
    const auto& name = member->name;
    const auto& value = member->value;
    if (name.IsString() && value.IsString()) {
      result.emplace(name.GetString(), value.GetString());
    }
  }

  return result;
}

template <>
PuffinFile::Footer::BlobMetadata Deserialize(const rapidjson::Value& document) {
  Ensure(document.IsObject(), std::string(__PRETTY_FUNCTION__));
  PuffinFile::Footer::BlobMetadata result;

  result.type = Extract<std::string>(document, field::kType);
  result.fields = Extract<std::vector<int32_t>>(document, field::kFields);
  result.snapshot_id = Extract<int64_t>(document, field::kSnapshotId);
  result.sequence_number = Extract<int64_t>(document, field::kSequenceNumber);
  result.offset = Extract<int64_t>(document, field::kOffset);
  result.length = Extract<int64_t>(document, field::kLength);
  result.compression_codec = Extract<std::optional<std::string>>(document, field::kCompressionCodec);
  result.properties = Extract<std::map<std::string, std::string>>(document, field::kProperties);

  return result;
}

template <>
PuffinFile::Footer::DeserializedFooter Deserialize(const rapidjson::Value& document) {
  Ensure(document.IsObject(), std::string(__PRETTY_FUNCTION__));
  PuffinFile::Footer::DeserializedFooter result;

  result.blobs = Extract<std::vector<PuffinFile::Footer::BlobMetadata>>(document, field::kBlobs);
  result.properties = Extract<std::map<std::string, std::string>>(document, field::kProperties);

  return result;
}

std::string CreateFooter(const PuffinFile::Footer::DeserializedFooter& footer) {
  serializer::Serializer serializer;
  rapidjson::Value value = serializer.Serialize(footer);
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  value.Accept(writer);

  return buffer.GetString();
}

}  // namespace serializer
}  // namespace

arrow::Result<PuffinFile::Footer> PuffinFile::MakeFooter(const std::string& data) {
  if (data.size() < 16) {
    return arrow::Status::ExecutionError("PuffinFile is incorrect: file is too small (", data.size(), ")");
  }
  std::string_view data_view = data;
  std::string_view magic_bytes_end = data_view.substr(data_view.size() - 4, 4);
  if (magic_bytes_end != kPuffinMagicBytes) {
    return arrow::Status::ExecutionError("PuffinFile is incorrect: magic bytes after footer are incorrect (expected ",
                                         kPuffinMagicBytes, ", found ", magic_bytes_end, ")");
  }
  std::string_view flags_bytes = data_view.substr(data_view.size() - 8, 4);
  std::string_view footer_payload_size_bytes = data_view.substr(data_view.size() - 12, 4);
  int32_t footer_payload_size = 0;
  std::memcpy(&footer_payload_size, footer_payload_size_bytes.data(), 4);

  uint32_t flags = 0;
  std::memcpy(&flags, flags_bytes.data(), 4);

  bool is_payload_compressed = flags & 1;
  if (is_payload_compressed) {
    return arrow::Status::ExecutionError("Compressed puffin files are not supported yet");
  }

  if (data.size() < 16 + footer_payload_size) {
    return arrow::Status::ExecutionError("PuffinFile is incorrect: file is too small (", data.size(),
                                         ", but footer payload size is ", footer_payload_size, ")");
  }
  std::string_view magic_bytes_begin = data_view.substr(data_view.size() - (16 + footer_payload_size), 4);
  if (magic_bytes_begin != kPuffinMagicBytes) {
    return arrow::Status::ExecutionError("PuffinFile is incorrect: magic bytes before footer are incorrect (expected ",
                                         kPuffinMagicBytes, ", found ", magic_bytes_end, ")");
  }

  std::string footer_payload = data.substr(data.size() - (12 + footer_payload_size), footer_payload_size);
  return PuffinFile::Footer(std::move(footer_payload));
}

PuffinFile::Footer::DeserializedFooter PuffinFile::Footer::GetDeserializedFooter() const {
  if (deserialized_footer_cache_.has_value()) {
    return deserialized_footer_cache_.value();
  }
  rapidjson::Document document;
  document.Parse(payload_.c_str(), payload_.length());

  deserialized_footer_cache_.emplace(serializer::Deserialize<PuffinFile::Footer::DeserializedFooter>(document));
  return deserialized_footer_cache_.value();
}

PuffinFile PuffinFileBuilder::Build() && {
  if (blobs_.empty()) {
    throw arrow::Status::ExecutionError("PuffinFileBuilder: building puffin file without blobs is not supported");
  }
  if (!sequence_number_.has_value()) {
    throw arrow::Status::ExecutionError("PuffinFileBuilder: sequence_number is not set");
  }
  if (!snapshot_id_.has_value()) {
    throw arrow::Status::ExecutionError("PuffinFileBuilder: snapshot_id is not set");
  }

  std::vector<PuffinFile::Footer::BlobMetadata> blob_meta;

  std::string result;
  result += kPuffinMagicBytes;

  for (auto& blob : blobs_) {
    PuffinFile::Footer::BlobMetadata meta;
    meta.fields = std::move(blob.fields);
    meta.length = blob.data.size();
    meta.offset = result.size();
    meta.properties = std::move(blob.properties);
    meta.sequence_number = sequence_number_.value();
    meta.snapshot_id = snapshot_id_.value();
    meta.type = blob.blob_type;

    result += blob.data;
    blob_meta.emplace_back(std::move(meta));
  }

  result += kPuffinMagicBytes;

  PuffinFile::Footer::DeserializedFooter deserialized_footer;
  deserialized_footer.blobs = std::move(blob_meta);
  deserialized_footer.properties["created-by"] = "iceberg-cpp";
  std::string footer = serializer::CreateFooter(deserialized_footer);

  result += footer;

  int32_t footer_size = footer.size();
  std::string serialized_footer_size(4, 0);
  std::memcpy(serialized_footer_size.data(), &footer_size, 4);

  result += serialized_footer_size;
  result += "0000";  // uncompressed flags

  result += kPuffinMagicBytes;

  auto maybe_puffin_file = PuffinFile::Make(std::move(result));
  if (!maybe_puffin_file.ok()) {
    throw maybe_puffin_file.status();
  }
  return maybe_puffin_file.MoveValueUnsafe();
}

}  // namespace iceberg
