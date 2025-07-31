#pragma once

#include "arrow/array/array_base.h"
#include "arrow/scalar.h"
#include "iceberg/type.h"
#include "rapidjson/document.h"

namespace iceberg {

class Literal {
 public:
  explicit Literal(std::shared_ptr<arrow::Scalar> scalar) : scalar_(scalar) {}
  std::shared_ptr<arrow::Array> MakeColumn(int64_t length) const;

  std::shared_ptr<arrow::Scalar> GetScalar() const {
    return scalar_;
  }

 private:
  std::shared_ptr<arrow::Scalar> scalar_;
};

std::shared_ptr<arrow::DataType> ConvertToDataType(std::shared_ptr<const types::Type> type);

Literal DeserializeLiteral(std::shared_ptr<const types::Type> type, const rapidjson::Value& document);

}  // namespace iceberg
