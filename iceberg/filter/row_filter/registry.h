#pragma once

#include "arrow/result.h"
#include "gandiva/node.h"
#include "iceberg/filter/representation/function.h"

namespace iceberg::filter {

class IGandivaFunctionRegistry {
 public:
  virtual arrow::Result<gandiva::NodePtr> CreateFunction(iceberg::filter::FunctionID function_id,
                                                         gandiva::NodeVector arguments) const = 0;

  virtual ~IGandivaFunctionRegistry() = default;
};

class GandivaFunctionRegistry : public IGandivaFunctionRegistry {
 public:
  explicit GandivaFunctionRegistry(int64_t timestamp_to_timestamptz_shift_us)
      : timestamp_to_timestamptz_shift_us_(timestamp_to_timestamptz_shift_us) {}

  arrow::Result<gandiva::NodePtr> CreateFunction(iceberg::filter::FunctionID function_id,
                                                 gandiva::NodeVector arguments) const override;

  void Promote(gandiva::NodePtr& l, gandiva::NodePtr& r) const;
  void PromoteLtoR(gandiva::NodePtr& l, gandiva::NodePtr& r) const;
  void PromoteToInt4(gandiva::NodePtr& gnode) const;
  void PromoteToInt8(gandiva::NodePtr& gnode) const;
  void PromoteToFloat8(gandiva::NodePtr& gnode) const;
  void PromoteToTimestamp(gandiva::NodePtr& gnode) const;
  void PromoteToTimestamptz(gandiva::NodePtr& gnode) const;

 private:
  int64_t timestamp_to_timestamptz_shift_us_;
};

}  // namespace iceberg::filter
