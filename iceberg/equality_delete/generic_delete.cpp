#include "iceberg/equality_delete/generic_delete.h"

#include <utility>

#include "arrow/array.h"
#include "iceberg/equality_delete/utils.h"

namespace iceberg {

namespace {
int GetWidth(const std::shared_ptr<arrow::Array>& array, int row) {
  int width = array->type()->byte_width();
  if (width != -1) {
    return width;
  }
  switch (array->type_id()) {
    case arrow::Type::STRING:
      return sizeof(uint32_t) + (static_cast<const arrow::StringArray*>(array.get())->GetView(row)).size();
    case arrow::Type::BINARY:
      return sizeof(uint32_t) + (static_cast<const arrow::BinaryArray*>(array.get())->GetView(row)).size();
    default:
      throw arrow::Status::NotImplemented("GetWidth: unexpected type");
  }
}

// clang-format off
template <typename T>
size_t WritePrimitiveToData(const T value, uint8_t* data)
requires(std::is_arithmetic_v<T>) {
  std::memcpy(data, &value, sizeof(value));
  return sizeof(value);
}
// clang-format on

size_t WriteStringViewToData(std::string_view value, uint8_t* data) {
  uint32_t sz = value.size();
  std::memcpy(data, &sz, sizeof(sz));
  std::memcpy(data + sizeof(uint32_t), value.data(), value.size());
  return (sizeof(sz) + value.size());
}

size_t WriteToData(const std::shared_ptr<arrow::Array>& array, int row, uint8_t* data) {
  switch (array->type_id()) {
    case arrow::Type::BOOL:
      return WritePrimitiveToData(static_cast<const arrow::BooleanArray*>(array.get())->GetView(row), data);
      break;
    case arrow::Type::INT8:
      return WritePrimitiveToData(static_cast<const arrow::Int8Array*>(array.get())->GetView(row), data);
      break;
    case arrow::Type::INT16:
      return WritePrimitiveToData(static_cast<const arrow::Int16Array*>(array.get())->GetView(row), data);
      break;
    case arrow::Type::INT32:
      return WritePrimitiveToData(static_cast<const arrow::Int32Array*>(array.get())->GetView(row), data);
      break;
    case arrow::Type::INT64:
      return WritePrimitiveToData(static_cast<const arrow::Int64Array*>(array.get())->GetView(row), data);
      break;
    case arrow::Type::FLOAT:
      return WritePrimitiveToData(static_cast<const arrow::FloatArray*>(array.get())->GetView(row), data);
      break;
    case arrow::Type::DOUBLE:
      return WritePrimitiveToData(static_cast<const arrow::DoubleArray*>(array.get())->GetView(row), data);
      break;
    case arrow::Type::STRING:
      return WriteStringViewToData(static_cast<const arrow::StringArray*>(array.get())->GetView(row), data);
    case arrow::Type::BINARY:
      return WriteStringViewToData(static_cast<const arrow::BinaryArray*>(array.get())->GetView(row), data);
      break;
    case arrow::Type::FIXED_SIZE_BINARY:
      return WriteStringViewToData(static_cast<const arrow::FixedSizeBinaryArray*>(array.get())->GetView(row), data);
      break;

    case arrow::Type::DATE32:
      return WritePrimitiveToData(static_cast<const arrow::Date32Array*>(array.get())->GetView(row), data);
      break;
    case arrow::Type::TIME64:
      return WritePrimitiveToData(static_cast<const arrow::Time64Array*>(array.get())->GetView(row), data);
      break;
    case arrow::Type::TIMESTAMP:
      return WritePrimitiveToData(static_cast<const arrow::TimestampArray*>(array.get())->GetView(row), data);
      break;

    case arrow::Type::DECIMAL128:
      return WriteStringViewToData(static_cast<const arrow::Decimal128Array*>(array.get())->GetView(row), data);
      break;
    case arrow::Type::LIST:
      return WriteStringViewToData(static_cast<const arrow::ListArray*>(array.get())->value_slice(row)->ToString(),
                                   data);
      break;
    default:
      throw arrow::Status::ExecutionError("WriteToData: unexpected type");
  }
}

}  // namespace

// TODO(gmusya): speed up this
GenericDeleteKey::GenericDeleteKey(const std::vector<std::shared_ptr<arrow::Array>>& arrays, int row,
                                   const std::shared_ptr<MemoryState>& shared_state)
    : shared_state_(shared_state) {
  uint64_t null_bitmask = 0;
  uint64_t total_width = 0;
  for (uint64_t i = 0; i < arrays.size(); ++i) {
    const auto& array = arrays[i];
    if (array->IsNull(row)) {
      null_bitmask |= (1ull << i);
    } else {
      total_width += GetWidth(array, row);
    }
  }
  shared_state->Allocate<uint32_t>();
  uint32_t size = sizeof(null_bitmask) + total_width;
  shared_state->AllocateArray<uint8_t>(size);
  uint8_t* data = new uint8_t[size];
  size_t first_unused_ = 0;
  first_unused_ += WritePrimitiveToData(null_bitmask, data);
  for (uint64_t i = 0; i < arrays.size(); ++i) {
    const auto& array = arrays[i];
    if (array->IsNull(row)) {
      continue;
    }
    first_unused_ += WriteToData(array, row, data + first_unused_);
  }
  sized_ptr_ = SizedPtr(data, size);
}

arrow::Status GenericEqualityDelete::Add(const std::vector<std::shared_ptr<arrow::Array>>& arrays, uint64_t rows_count,
                                         Layer delete_layer) {
  if (arrays.size() > 64) {
    throw arrow::Status::NotImplemented("GenericEqualityDelete is not implemented for more than 64 columns");
  }
  for (uint64_t row = 0; row < rows_count; ++row) {
    safe::UpdateMax(values_, GenericDeleteKey(arrays, row, shared_state_), delete_layer);
  }
  return arrow::Status::OK();
}

bool GenericEqualityDelete::IsDeleted(const std::vector<std::shared_ptr<arrow::Array>>& arrays, uint64_t row,
                                      Layer data_layer) const {
  auto key = GenericDeleteKey(arrays, row, shared_state_);
  std::optional<Layer> delete_layer = values_.Get(key);
  return delete_layer && *delete_layer >= data_layer;
}

size_t GenericEqualityDelete::Size() const { return values_.Size(); }

}  // namespace iceberg
