#pragma once

#include <memory>
#include <vector>

#include "arrow/status.h"
#include "iceberg/equality_delete/delete.h"
#include "iceberg/equality_delete/utils.h"

namespace iceberg {

template <typename ArrayType>
class SpecializedDeleteOneColumn final : public EqualityDelete {
  using ValueType = typename ArrayType::value_type;

 public:
  explicit SpecializedDeleteOneColumn(arrow::Type::type type, const std::shared_ptr<MemoryState>& shared_state)
      : type_(type), values_(shared_state), shared_state_(shared_state) {}

  inline void Reserve(uint64_t rows) { values_.Reserve(rows); }

  arrow::Status Add(const std::vector<std::shared_ptr<arrow::Array>>& arrays, uint64_t rows_count,
                    Layer delete_layer) override {
    if (arrays.size() != 1) {
      throw arrow::Status::ExecutionError("SpecializedDeleteOneColumn (Add): unexpected number of columns");
    }
    const auto& array = arrays[0];
    if (array->type_id() != type_) {
      return arrow::Status::ExecutionError("SpecializedDeleteOneColumn (Add): unexpected type");
    }
    for (uint64_t row = 0; row < rows_count; ++row) {
      if (array->IsNull(row)) {
        contains_null_ = true;
      } else {
        safe::ExceptionFlagGuard guard(shared_state_, true);

        safe::UpdateMax(values_, static_cast<const ArrayType*>(array.get())->GetView(row), delete_layer);
      }
    }
    return arrow::Status::OK();
  }

  size_t Size() const override { return values_.Size() + (contains_null_ ? 1 : 0); }

  bool IsDeleted(const std::vector<std::shared_ptr<arrow::Array>>& arrays, uint64_t row,
                 Layer data_layer) const override {
    if (arrays[0]->IsNull(row)) {
      return contains_null_;
    }
    auto key = static_cast<const ArrayType*>(arrays[0].get())->GetView(row);
    std::optional<Layer> delete_layer = values_.Get(key);
    return delete_layer && *delete_layer >= data_layer;
  }

 private:
  arrow::Type::type type_;
  bool contains_null_ = false;
  safe::FlatHashMap<ValueType, Layer> values_;
  std::shared_ptr<MemoryState> shared_state_;
};

}  // namespace iceberg
