#pragma once

#include <bit>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/status.h"
#include "iceberg/equality_delete/common.h"
#include "iceberg/equality_delete/delete.h"
#include "iceberg/equality_delete/utils.h"

namespace iceberg {

template <typename ValueType>
class SpecializedDeleteMultipleColumn final : public EqualityDelete {
 public:
  explicit SpecializedDeleteMultipleColumn(std::vector<arrow::Type::type> types, std::vector<int> order,
                                           const std::shared_ptr<MemoryState>& shared_state)
      : types_(std::move(types)),
        order_(std::move(order)),
        bit_width_([&]() -> std::vector<int> {
          std::vector<int> result;
          for (arrow::Type::type type : types_) {
            result.emplace_back(TypeToBitWidth(type));
          }
          return result;
        }()),
        values_(shared_state),
        shared_state_(shared_state) {}

  inline void Reserve(uint64_t rows) { values_.Reserve(rows); }

  arrow::Status Add(const std::vector<std::shared_ptr<arrow::Array>>& arrays, uint64_t rows_count,
                    Layer delete_layer) override {
    if (arrays.size() != types_.size()) {
      return arrow::Status::ExecutionError(
          "SpecializedDeleteMultipleColumn (Add): unexpected number of "
          "columns");
    }
    bool with_nulls = false;
    for (size_t i = 0; i < types_.size(); ++i) {
      if (arrays[i]->type_id() != types_[i]) {
        return arrow::Status::ExecutionError("SpecializedDeleteMultipleColumn (Add): unexpected type");
      }
      if (arrays[i]->null_count() > 0) {
        with_nulls = true;
      }
    }
    if (!with_nulls) {
      for (uint64_t row = 0; row < rows_count; ++row) {
        if (auto allocation_status = shared_state_->AllocateWithStatus<ValueType>(); !allocation_status.ok()) {
          return allocation_status;
        }
        ++allocated_;
        safe::UpdateMax(values_, ConstructRow(arrays, row), delete_layer);
      }
    } else {
      if (arrays.size() >= 9) {
        return arrow::Status::NotImplemented(
            "Equality deletes with nulls and more than 8 columns are not "
            "supported");
      }
      for (uint64_t row = 0; row < rows_count; ++row) {
        uint8_t nulls_mask = 0;
        for (int id : order_) {
          if (arrays[id]->IsNull(row)) {
            nulls_mask |= (1 << id);
          }
        }
        if (auto allocation_status = shared_state_->AllocateWithStatus<ValueType>(); !allocation_status.ok()) {
          return allocation_status;
        }
        ++allocated_;
        ValueType key = ConstructRowIgnoreNulls(arrays, row);
        if (nulls_mask == 0) {
          safe::UpdateMax(values_, std::move(key), delete_layer);
        } else {
          if (!values_with_nulls_.contains(nulls_mask)) {
            values_with_nulls_.emplace(nulls_mask, safe::FlatHashMap<ValueType, Layer>(shared_state_));
          }
          safe::UpdateMax(values_with_nulls_.at(nulls_mask), std::move(key), delete_layer);
        }
      }
    }
    return arrow::Status::OK();
  }

  size_t Size() const override {
    size_t result = values_.Size();
    for (const auto& [bitmap, values] : values_with_nulls_) {
      result += values.Size();
    }
    return result;
  }

  bool IsDeleted(const std::vector<std::shared_ptr<arrow::Array>>& arrays, uint64_t row,
                 Layer data_layer) const override {
    bool with_null = false;
    for (const auto& array : arrays) {
      if (array->IsNull(row)) {
        with_null = true;
        break;
      }
    }
    if (!with_null) {
      ValueType value = ConstructRow(arrays, row);
      std::optional<Layer> delete_layer = values_.Get(value);
      return delete_layer && *delete_layer >= data_layer;
    } else {
      uint8_t nulls_mask = 0;
      for (int id : order_) {
        if (arrays[id]->IsNull(row)) {
          nulls_mask |= (1 << id);
        }
      }
      auto it = values_with_nulls_.find(nulls_mask);
      if (it == values_with_nulls_.end()) {
        return false;
      }
      ValueType value = ConstructRowIgnoreNulls(arrays, row);
      std::optional<Layer> delete_layer = it->second.Get(value);
      return delete_layer && *delete_layer >= data_layer;
    }
  }

  ~SpecializedDeleteMultipleColumn() {
    if (shared_state_) {
      shared_state_->DeallocateArray<ValueType>(allocated_);
    }
  }

 private:
  ValueType ConstructRow(const std::vector<std::shared_ptr<arrow::Array>>& arrays, int row) const {
    if (auto allocation_status = shared_state_->AllocateWithStatus<ValueType>(); !allocation_status.ok()) {
      throw allocation_status;
    }
    ++allocated_;
    ValueType result = 0;
    int current_shift = 0;
    for (int id : order_) {
      if (arrays[id]->IsNull(row)) {
        continue;
      }
      ValueType new_value;
      switch (types_[id]) {
        case arrow::Type::INT16:
          new_value = std::bit_cast<uint16_t>(GetValue<arrow::Int16Array>(arrays[id], row));
          break;
        case arrow::Type::INT32:
          new_value = std::bit_cast<uint32_t>(GetValue<arrow::Int32Array>(arrays[id], row));
          break;
        case arrow::Type::INT64:
          new_value = std::bit_cast<uint64_t>(GetValue<arrow::Int64Array>(arrays[id], row));
          break;
        case arrow::Type::TIMESTAMP:
          new_value = std::bit_cast<uint64_t>(GetValue<arrow::TimestampArray>(arrays[id], row));
          break;
        default:
          throw arrow::Status::ExecutionError("Internal error in " + std::string(__PRETTY_FUNCTION__) +
                                              ": Unexpected type");
      }
      result += new_value << current_shift;
      current_shift += bit_width_[id];
    }
    return result;
  }

  ValueType ConstructRowIgnoreNulls(const std::vector<std::shared_ptr<arrow::Array>>& arrays, int row) const {
    if (auto allocation_status = shared_state_->AllocateWithStatus<ValueType>(); !allocation_status.ok()) {
      throw allocation_status;
    }
    ++allocated_;
    ValueType result = 0;
    int current_shift = 0;
    for (int id : order_) {
      ValueType new_value;
      switch (types_[id]) {
        case arrow::Type::INT16:
          new_value = std::bit_cast<uint16_t>(GetValue<arrow::Int16Array>(arrays[id], row));
          break;
        case arrow::Type::INT32:
          new_value = std::bit_cast<uint32_t>(GetValue<arrow::Int32Array>(arrays[id], row));
          break;
        case arrow::Type::INT64:
          new_value = std::bit_cast<uint64_t>(GetValue<arrow::Int64Array>(arrays[id], row));
          break;
        case arrow::Type::TIMESTAMP:
          new_value = std::bit_cast<uint64_t>(GetValue<arrow::TimestampArray>(arrays[id], row));
          break;
        default:
          throw arrow::Status::ExecutionError("Internal error in " + std::string(__PRETTY_FUNCTION__) +
                                              ": Unexpected type");
      }
      result += new_value << current_shift;
      current_shift += bit_width_[id];
    }
    return result;
  }

  template <typename ArrayType>
  inline typename ArrayType::value_type GetValue(const std::shared_ptr<arrow::Array>& array, int row) const {
    return static_cast<const ArrayType*>(array.get())->GetView(row);
  }

  const std::vector<arrow::Type::type> types_;
  const std::vector<int> order_;
  const std::vector<int> bit_width_;
  mutable size_t allocated_ = 0;

  safe::FlatHashMap<ValueType, Layer> values_;

  std::shared_ptr<MemoryState> shared_state_;
  std::map<uint8_t, safe::FlatHashMap<ValueType, Layer>> values_with_nulls_;
};

}  // namespace iceberg
