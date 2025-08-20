#include "iceberg/literals.h"

#include <iostream>

#include "arrow/api.h"
#include "arrow/array/util.h"
#include "arrow/util/value_parsing.h"
#include "iceberg/common/error.h"

namespace iceberg {
namespace {

int64_t ParseTime(const rapidjson::Value& document, arrow::TimeUnit::type type) {
  Ensure(document.IsString(), std::string(__PRETTY_FUNCTION__) + ": Expected String value");
  int64_t dat;
  auto parser = arrow::TimestampParser::MakeISO8601();
  Ensure(parser->operator()(document.GetString(), document.GetStringLength(), type, &dat),
         std::string(__PRETTY_FUNCTION__) + ": Failed to parse Date/Timestamp");
  return dat;
}

std::unique_ptr<arrow::Buffer> DeserializeHexadecimal(const char* str, int length) {
  Ensure(length % 2 == 0, std::string(__PRETTY_FUNCTION__) + ": Expected even number of characters");

  std::string_view str_view(str, length);
  auto maybe_buffer = arrow::AllocateBuffer(length / 2);
  Ensure(maybe_buffer.ok(), std::string(__PRETTY_FUNCTION__) + ": failed to allocate arrow::Buffer");
  auto buffer = maybe_buffer.MoveValueUnsafe();
  uint8_t* data = buffer->mutable_data();

  for (int i = 0; i < length / 2; ++i) {
    data[i] = static_cast<uint8_t>(std::stoi(std::string(str_view.substr(i * 2, 2)), nullptr, 16));
  }
  return buffer;
}

std::shared_ptr<arrow::DataType> ConvertToDataType(std::shared_ptr<const types::Type> type) {
  switch (type->TypeId()) {
    case TypeID::kBoolean:
      return std::make_shared<arrow::BooleanType>();
    case TypeID::kInt:
      return std::make_shared<arrow::Int32Type>();
    case TypeID::kLong:
      return std::make_shared<arrow::Int64Type>();
    case TypeID::kFloat:
      return std::make_shared<arrow::FloatType>();
    case TypeID::kDouble:
      return std::make_shared<arrow::DoubleType>();
    case TypeID::kDecimal: {
      auto decimal = std::static_pointer_cast<const types::DecimalType>(type);
      return std::make_shared<arrow::Decimal128Type>(decimal->Precision(), decimal->Scale());
    }
    case TypeID::kDate:
      return std::make_shared<arrow::Date32Type>();
    case TypeID::kTime:
      return std::make_shared<arrow::Time64Type>(arrow::TimeUnit::MICRO);
    case TypeID::kTimestamp:
      return std::make_shared<arrow::TimestampType>(arrow::TimeUnit::MICRO);
    case TypeID::kTimestamptz:
      return std::make_shared<arrow::TimestampType>(arrow::TimeUnit::MICRO, "UTC");
    case TypeID::kTimestampNs:
      return std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO);
    case TypeID::kTimestamptzNs:
      return std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO, "UTC");
    case TypeID::kString:
      return std::make_shared<arrow::StringType>();
    case TypeID::kUuid:
      return std::make_shared<arrow::FixedSizeBinaryType>(16);
    case TypeID::kFixed:
      return std::make_shared<arrow::FixedSizeBinaryType>(
          std::static_pointer_cast<const types::FixedType>(type)->Size());
    case TypeID::kBinary:
      return std::make_shared<arrow::BinaryType>();
    case TypeID::kList:
      return std::make_shared<arrow::ListType>(
          ConvertToDataType(std::static_pointer_cast<const types::ListType>(type)->ElementType()));
    default:
      Ensure(false, std::string(__PRETTY_FUNCTION__) + ": Unsupported type");
  }
}

std::shared_ptr<arrow::Scalar> DeserializeScalar(std::shared_ptr<const types::Type> type,
                                                 const rapidjson::Value& document) {
  switch (type->TypeId()) {
    case TypeID::kBoolean:
      return std::make_shared<arrow::BooleanScalar>(document.GetBool());
    case TypeID::kInt:
      Ensure(document.IsInt(), std::string(__PRETTY_FUNCTION__) + ": Expected Int value");
      return std::make_shared<arrow::Int32Scalar>(document.GetInt());
    case TypeID::kLong:
      Ensure(document.IsInt64(), std::string(__PRETTY_FUNCTION__) + ": Expected Int64 value");
      return std::make_shared<arrow::Int64Scalar>(document.GetInt64());
    case TypeID::kFloat:
      Ensure(document.IsFloat(), std::string(__PRETTY_FUNCTION__) + ": Expected Float value");
      return std::make_shared<arrow::FloatScalar>(document.GetFloat());
    case TypeID::kDouble:
      Ensure(document.IsDouble(), std::string(__PRETTY_FUNCTION__) + ": Expected Double value");
      return std::make_shared<arrow::DoubleScalar>(document.GetDouble());
    case TypeID::kDecimal: {
      Ensure(document.IsString(), std::string(__PRETTY_FUNCTION__) + ": Expected String value");
      auto decimal = std::static_pointer_cast<const types::DecimalType>(type);
      auto precision = decimal->Precision();
      auto scale = decimal->Scale();
      std::string str = document.GetString();
      if (scale < 0) {
        auto it = str.find("E+");
        Ensure(it != std::string::npos,
               std::string(__PRETTY_FUNCTION__) + ": Negative scale values must be presented in scientific notation");
        auto str_scale = std::stoi(str.substr(it + 2));
        Ensure(str_scale == -scale, ": The exponent must equal the negated scale");
        str = str.substr(0, it) + std::string(str_scale, '0');
        precision += str_scale;
        scale = 0;
      }
      constexpr int kMaxPrecision = 38;

      Ensure(precision <= kMaxPrecision, std::string(__PRETTY_FUNCTION__) + ": Decimal precision greater than " +
                                             std::to_string(kMaxPrecision) + " is unsupported");

      auto maybe_decimal_value = arrow::Decimal128::FromString(str);
      Ensure(maybe_decimal_value.ok(), std::string(__PRETTY_FUNCTION__) + ": failed to convert string to decimal");
      auto decimal_value = maybe_decimal_value.MoveValueUnsafe();
      auto type = std::make_shared<arrow::Decimal128Type>(precision, scale);
      return std::make_shared<arrow::Decimal128Scalar>(decimal_value, type);
    }
    case TypeID::kDate: {
      int64_t sec_since_epoch = ParseTime(document, arrow::TimeUnit::SECOND);
      return std::make_shared<arrow::Date32Scalar>(sec_since_epoch / 86400);
    }
    case TypeID::kTime: {
      Ensure(document.IsString(), std::string(__PRETTY_FUNCTION__) + ": Expected String value");
      int64_t micros_since_epoch;
      std::string str = "1970-01-01T" + std::string(document.GetString());
      auto parser = arrow::TimestampParser::MakeISO8601();
      Ensure(parser->operator()(str.data(), str.size(), arrow::TimeUnit::MICRO, &micros_since_epoch),
             std::string(__PRETTY_FUNCTION__) + ": Failed to parse Time");
      return std::make_shared<arrow::Time64Scalar>(micros_since_epoch, arrow::TimeUnit::MICRO);
    }
    case TypeID::kTimestamp: {
      int64_t micros_since_epoch = ParseTime(document, arrow::TimeUnit::MICRO);
      return std::make_shared<arrow::TimestampScalar>(micros_since_epoch, arrow::TimeUnit::MICRO);
    }
    case TypeID::kTimestamptz: {
      int64_t micros_since_epoch = ParseTime(document, arrow::TimeUnit::MICRO);
      return std::make_shared<arrow::TimestampScalar>(micros_since_epoch, arrow::TimeUnit::MICRO, "UTC");
    }
    case TypeID::kTimestampNs: {
      int64_t nanos_since_epoch = ParseTime(document, arrow::TimeUnit::NANO);
      return std::make_shared<arrow::TimestampScalar>(nanos_since_epoch, arrow::TimeUnit::NANO);
    }
    case TypeID::kTimestamptzNs: {
      int64_t nanos_since_epoch = ParseTime(document, arrow::TimeUnit::NANO);
      return std::make_shared<arrow::TimestampScalar>(nanos_since_epoch, arrow::TimeUnit::NANO, "UTC");
    }
    case TypeID::kString:
      Ensure(document.IsString(), std::string(__PRETTY_FUNCTION__) + ": Expected String value");
      return std::make_shared<arrow::StringScalar>(document.GetString());
    case TypeID::kUuid: {
      Ensure(document.IsString(), std::string(__PRETTY_FUNCTION__) + ": Expected String value");

      std::string clean_uuid;
      auto length = document.GetStringLength();
      const std::string_view str_view = document.GetString();
      for (int i = 0; i < length; ++i) {
        if (str_view[i] != '-') {
          clean_uuid += str_view[i];
        }
      }

      auto buffer = DeserializeHexadecimal(clean_uuid.data(), clean_uuid.size());
      constexpr int uuid_length = 16;
      Ensure(buffer->size() == uuid_length,
             std::string(__PRETTY_FUNCTION__) + ": Expected " + std::to_string(uuid_length) + " bytes for UUID");

      return std::make_shared<arrow::FixedSizeBinaryScalar>(std::shared_ptr<arrow::Buffer>(buffer.release()),
                                                            arrow::fixed_size_binary(uuid_length));
    }
    case TypeID::kFixed: {
      Ensure(document.IsString(), std::string(__PRETTY_FUNCTION__) + ": Expected String value");
      auto buffer = DeserializeHexadecimal(document.GetString(), document.GetStringLength());

      size_t size = std::static_pointer_cast<const types::FixedType>(type)->Size();
      Ensure(size == buffer->size(),
             std::string(__PRETTY_FUNCTION__) + ": Expected String length of " + std::to_string(size));

      return std::make_shared<arrow::FixedSizeBinaryScalar>(std::shared_ptr<arrow::Buffer>(buffer.release()),
                                                            arrow::fixed_size_binary(size));
    }
    case TypeID::kBinary: {
      Ensure(document.IsString(), std::string(__PRETTY_FUNCTION__) + ": Expected String value");

      auto buffer = DeserializeHexadecimal(document.GetString(), document.GetStringLength());
      return std::make_shared<arrow::BinaryScalar>(std::shared_ptr<arrow::Buffer>(buffer.release()), arrow::binary());
    }
    case TypeID::kList: {
      Ensure(document.IsArray(), std::string(__PRETTY_FUNCTION__) + ": Expected String value");
      auto element_type = std::static_pointer_cast<const types::ListType>(type)->ElementType();
      int size = document.Size();
      std::vector<std::shared_ptr<arrow::Scalar>> scalars(size);
      for (int i = 0; i < size; ++i) {
        scalars[i] = DeserializeScalar(element_type, document[i]);
      }

      std::unique_ptr<arrow::ArrayBuilder> builder;
      Ensure(arrow::MakeBuilder(arrow::default_memory_pool(), ConvertToDataType(element_type), &builder) ==
                 arrow::Status::OK(),
             std::string(__PRETTY_FUNCTION__) + ": Failed to create ArrayBuilder");
      Ensure(builder->AppendScalars(scalars) == arrow::Status::OK(),
             std::string(__PRETTY_FUNCTION__) + ": Failed to add scalars to ArrayBuilder");
      std::shared_ptr<arrow::Array> out;
      Ensure(builder->Finish(&out) == arrow::Status::OK(),
             std::string(__PRETTY_FUNCTION__) + ": Failed to get an Array object from ArrayBuilder");
      return std::make_shared<arrow::ListScalar>(out);
    }
    default:
      Ensure(false, std::string(__PRETTY_FUNCTION__) + ": Unsupported type");
  }
}

}  // namespace

std::shared_ptr<arrow::Array> Literal::MakeColumn(int64_t length) const {
  auto maybe_array = MakeArrayFromScalar(*scalar_, length);
  Ensure(maybe_array.ok(), std::string(__PRETTY_FUNCTION__) + ": failed to create array from scalar");
  return maybe_array.MoveValueUnsafe();
}

Literal DeserializeLiteral(std::shared_ptr<const types::Type> type, const rapidjson::Value& document) {
  return Literal(DeserializeScalar(type, document));
}

}  // namespace iceberg
