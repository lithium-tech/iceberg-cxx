#pragma once

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/array_primitive.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/record_batch.h"
#include "arrow/type_fwd.h"
#include "iceberg/result.h"
#include "iceberg/test_utils/optional_vector.h"

namespace iceberg {

template <typename BuilderType, typename ValueType>
std::shared_ptr<arrow::Array> MakeColumn(const std::vector<ValueType>& values) {
  BuilderType builder;
  Ensure(builder.Reserve(values.size()));
  for (const auto& v : values) {
    if (v) {
      Ensure(builder.Append(*v));
    } else {
      Ensure(builder.AppendNull());
    }
  }

  auto array = iceberg::ValueSafe(builder.Finish());
  return array;
}

inline std::shared_ptr<arrow::Array> MakeInt32ArrowColumn(const OptionalVector<int32_t>& values) {
  return MakeColumn<arrow::Int32Builder>(values);
}

inline std::shared_ptr<arrow::Array> MakeInt16ArrowColumn(const OptionalVector<int16_t>& values) {
  return MakeColumn<arrow::Int16Builder>(values);
}

inline std::shared_ptr<arrow::Array> MakeStringArrowColumn(const std::vector<std::string*>& values) {
  return MakeColumn<arrow::StringBuilder>(values);
}

inline std::shared_ptr<arrow::RecordBatch> MakeBatch(const std::vector<std::shared_ptr<arrow::Array>>& arrays,
                                                     const std::vector<std::string>& names) {
  Ensure(names.size() == arrays.size() && names.size() > 0, "Error in test");

  arrow::FieldVector fields;
  for (size_t i = 0; i < arrays.size(); ++i) {
    fields.emplace_back(std::make_shared<arrow::Field>(names.at(i), arrays.at(i)->type()));
  }

  auto schema = std::make_shared<arrow::Schema>(std::move(fields));

  return arrow::RecordBatch::Make(schema, arrays.at(0)->length(), std::move(arrays));
}

}  // namespace iceberg
