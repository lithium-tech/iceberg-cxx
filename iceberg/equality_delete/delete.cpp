#include "iceberg/equality_delete/delete.h"

#include <algorithm>
#include <numeric>
#include <utility>
#include <vector>

#include "absl/numeric/int128.h"
#include "iceberg/equality_delete/common.h"
#include "iceberg/equality_delete/generic_delete.h"
#include "iceberg/equality_delete/specialized_multiple_column_delete.h"
#include "iceberg/equality_delete/specialized_one_column_delete.h"

namespace iceberg {

EqualityDeletePtr MakeEqualityDelete(std::shared_ptr<arrow::Schema> schema, bool use_specialized_deletes,
                                     uint64_t num_rows, const std::shared_ptr<MemoryState>& shared_state) {
  if (!use_specialized_deletes) {
    return std::make_unique<GenericEqualityDelete>(shared_state);
  }
  const auto& fields = schema->fields();
  if (fields.size() == 1) {
    const auto& field = fields[0];
    switch (field->type()->id()) {
      case arrow::Type::INT16: {
        auto ptr = std::make_unique<SpecializedDeleteOneColumn<arrow::Int16Array>>(arrow::Type::INT16, shared_state);
        ptr->Reserve(num_rows);
        return ptr;
      }
      case arrow::Type::INT32: {
        auto ptr = std::make_unique<SpecializedDeleteOneColumn<arrow::Int32Array>>(arrow::Type::INT32, shared_state);
        ptr->Reserve(num_rows);
        return ptr;
      }
      case arrow::Type::INT64: {
        auto ptr = std::make_unique<SpecializedDeleteOneColumn<arrow::Int64Array>>(arrow::Type::INT64, shared_state);
        ptr->Reserve(num_rows);
        return ptr;
      }
      case arrow::Type::TIMESTAMP: {
        auto ptr =
            std::make_unique<SpecializedDeleteOneColumn<arrow::TimestampArray>>(arrow::Type::TIMESTAMP, shared_state);
        ptr->Reserve(num_rows);
        return ptr;
      }
      default:
        return std::make_unique<GenericEqualityDelete>(shared_state);
    }
  }
  bool all_types_good = true;
  std::vector<arrow::Type::type> types;
  std::vector<int> widths;
  for (const auto& field : fields) {
    const auto type_id = field->type()->id();
    switch (type_id) {
      case arrow::Type::INT16:
      case arrow::Type::INT32:
      case arrow::Type::INT64:
      case arrow::Type::TIMESTAMP:
        types.push_back(type_id);
        widths.push_back(TypeToBitWidth(type_id));
        break;
      default:
        all_types_good = false;
        break;
    }
  }
  uint32_t total_width = 0;
  for (const auto& width : widths) {
    total_width += width;
  }
  if (!all_types_good || total_width > 128) {
    return std::make_unique<GenericEqualityDelete>(shared_state);
  } else {
    std::vector<int> order(widths.size());
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(), [&](int lhs, int rhs) { return widths[lhs] > widths[rhs]; });
    if (total_width <= 32) {
      auto ptr =
          std::make_unique<SpecializedDeleteMultipleColumn<uint32_t>>(std::move(types), std::move(order), shared_state);
      ptr->Reserve(num_rows);
      return ptr;
    } else if (total_width <= 64) {
      auto ptr =
          std::make_unique<SpecializedDeleteMultipleColumn<uint64_t>>(std::move(types), std::move(order), shared_state);
      ptr->Reserve(num_rows);
      return ptr;
    } else if (total_width <= 128) {
      auto ptr = std::make_unique<SpecializedDeleteMultipleColumn<absl::uint128>>(std::move(types), std::move(order),
                                                                                  shared_state);
      ptr->Reserve(num_rows);
      return ptr;
    } else {
      throw arrow::Status::ExecutionError("Internal error in tea. Unexpected total_bit_width");
    }
  }
}

}  // namespace iceberg
