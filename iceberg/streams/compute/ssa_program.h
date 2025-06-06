#pragma once

#include <optional>

#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "arrow/compute/api_aggregate.h"
#include "iceberg/filter/representation/function.h"

namespace iceberg::compute {

using SsaOperation = iceberg::filter::FunctionID;

const char* GetFunctionName(SsaOperation op);
std::optional<SsaOperation> ValidateOperation(SsaOperation op, uint32_t argsSize);

class Assign {
 public:
  Assign(const std::string& name_, SsaOperation op, std::vector<std::string>&& args)
      : name(name_), operation(ValidateOperation(op, args.size())), arguments(std::move(args)), funcOpts(nullptr) {}

  Assign(const std::string& name_, SsaOperation op, std::vector<std::string>&& args,
         std::shared_ptr<arrow::compute::FunctionOptions> funcOpts)
      : name(name_), operation(ValidateOperation(op, args.size())), arguments(std::move(args)), funcOpts(funcOpts) {}

  explicit Assign(const std::string& name_, bool value)
      : name(name_), constant(std::make_shared<arrow::BooleanScalar>(value)), funcOpts(nullptr) {}

  explicit Assign(const std::string& name_, int32_t value)
      : name(name_), constant(std::make_shared<arrow::Int32Scalar>(value)), funcOpts(nullptr) {}

  explicit Assign(const std::string& name_, uint32_t value)
      : name(name_), constant(std::make_shared<arrow::UInt32Scalar>(value)), funcOpts(nullptr) {}

  explicit Assign(const std::string& name_, int64_t value)
      : name(name_), constant(std::make_shared<arrow::Int64Scalar>(value)), funcOpts(nullptr) {}

  explicit Assign(const std::string& name_, uint64_t value)
      : name(name_), constant(std::make_shared<arrow::UInt64Scalar>(value)), funcOpts(nullptr) {}

  explicit Assign(const std::string& name_, float value)
      : name(name_), constant(std::make_shared<arrow::FloatScalar>(value)), funcOpts(nullptr) {}

  explicit Assign(const std::string& name_, double value)
      : name(name_), constant(std::make_shared<arrow::DoubleScalar>(value)), funcOpts(nullptr) {}

  explicit Assign(const std::string& name_, const std::string& value)
      : name(name_), constant(std::make_shared<arrow::StringScalar>(value)), funcOpts(nullptr) {}

  Assign(const std::string& name_, const std::shared_ptr<arrow::Scalar>& value)
      : name(name_), constant(value), funcOpts(nullptr) {}

  bool IsConstant() const { return constant.get(); }
  bool IsOk() const { return operation.has_value() || constant; }
  SsaOperation GetOperation() const { return *operation; }
  const std::vector<std::string>& GetArguments() const { return arguments; }
  std::shared_ptr<arrow::Scalar> GetConstant() const { return constant; }
  const std::string& GetName() const { return name; }
  const arrow::compute::FunctionOptions* GetFunctionOptions() const { return funcOpts.get(); }

 private:
  std::string name;
  std::shared_ptr<arrow::Scalar> constant;  // either constant or operation is set, not both of them
  std::optional<SsaOperation> operation;
  std::vector<std::string> arguments;
  std::shared_ptr<arrow::compute::FunctionOptions> funcOpts;
};

/// Group of commands that finishes with projection. Steps add locality for columns definition.
///
/// In step we have non-decreasing count of columns (line to line) till projection. So columns are either source
/// for the step either defined in this step.
/// It's also possible to use several filters in step. They would be applyed after assignes, just before projection.
/// "Filter (a > 0 AND b <= 42)" is logically equal to "Filret a > 0; Filter b <= 42"
/// Step combines (f1 AND f2 AND ... AND fn) into one filter and applies it once. You have to split filters in different
/// steps if you want to run them separately. I.e. if you expect that f1 is fast and leads to a small row-set.
/// Then when we place all assignes before filters they have the same row count. It's possible to run them in parallel.
struct ProgramStep {
  std::vector<Assign> assignes;
  std::vector<std::string> filters;     // List of filter columns. Implicit "Filter by (f1 AND f2 AND .. AND fn)"
  std::vector<std::string> projection;  // Step's result columns (remove others)

  struct DatumBatch {
    std::shared_ptr<arrow::Schema> schema;
    std::vector<arrow::Datum> datums;
    int64_t rows{};

    arrow::Status AddColumn(const std::string& name, arrow::Datum&& column);
    arrow::Result<arrow::Datum> GetColumnByName(const std::string& name) const;
    std::shared_ptr<arrow::RecordBatch> ToRecordBatch() const;
    static std::shared_ptr<ProgramStep::DatumBatch> FromRecordBatch(std::shared_ptr<arrow::RecordBatch>& batch);
  };

  bool Empty() const { return assignes.empty() && filters.empty() && projection.empty(); }

  arrow::Status Apply(std::shared_ptr<arrow::RecordBatch>& batch, arrow::compute::ExecContext* ctx) const;

  arrow::Status ApplyAssignes(DatumBatch& batch, arrow::compute::ExecContext* ctx) const;
  arrow::Status ApplyFilters(DatumBatch& batch) const;
  arrow::Status ApplyProjection(std::shared_ptr<arrow::RecordBatch>& batch) const;
  arrow::Status ApplyProjection(DatumBatch& batch) const;
};

struct Program {
  std::vector<std::shared_ptr<ProgramStep>> steps;

  Program() = default;
  Program(std::vector<std::shared_ptr<ProgramStep>>&& steps_) : steps(std::move(steps_)) {}

  arrow::Status ApplyTo(std::shared_ptr<arrow::RecordBatch>& batch, arrow::compute::ExecContext* ctx) const {
    try {
      for (auto& step : steps) {
        auto status = step->Apply(batch, ctx);
        if (!status.ok()) return status;
      }
    } catch (const std::exception& ex) {
      return arrow::Status::Invalid(ex.what());
    }
    return arrow::Status::OK();
  }
};

inline arrow::Status ApplyProgram(std::shared_ptr<arrow::RecordBatch>& batch, const Program& program,
                                  arrow::compute::ExecContext* ctx = nullptr) {
  return program.ApplyTo(batch, ctx);
}

}  // namespace iceberg::compute
