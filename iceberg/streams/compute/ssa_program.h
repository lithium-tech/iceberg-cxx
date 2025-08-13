#pragma once

#include <stdexcept>
#include <variant>

#include "arrow/api.h"
#include "arrow/compute/api.h"
#include "arrow/compute/api_aggregate.h"
#include "iceberg/common/error.h"
#include "iceberg/filter/representation/function.h"

namespace iceberg::compute {

using SsaOperation = iceberg::filter::FunctionID;

const char* GetFunctionName(SsaOperation op);
bool IsOperationValid(SsaOperation op, size_t num_args);

inline void ValidateOperation(SsaOperation op, size_t num_args) {
  Ensure(IsOperationValid(op, num_args), "Wrong arguments count for function");
}

class Assign {
 public:
  Assign(const std::string& name_, SsaOperation op, std::vector<std::string>&& args,
         std::shared_ptr<arrow::compute::FunctionOptions> func_opts = {})
      : name(name_), assignment(op), arguments(std::move(args)), func_opts(func_opts) {
    ValidateOperation(op, arguments.size());
  }

  Assign(const std::string& name_, const std::shared_ptr<arrow::Scalar>& value)
      : name(name_), assignment(value), func_opts(nullptr) {}

  bool IsConstant() const { return std::holds_alternative<std::shared_ptr<arrow::Scalar>>(assignment); }
  bool IsOperation() const { return std::holds_alternative<SsaOperation>(assignment); }
  SsaOperation GetOperation() const { return std::get<SsaOperation>(assignment); }
  std::shared_ptr<arrow::Scalar> GetConstant() const { return std::get<std::shared_ptr<arrow::Scalar>>(assignment); }
  const std::vector<std::string>& GetArguments() const { return arguments; }
  const std::string& GetName() const { return name; }
  const arrow::compute::FunctionOptions* GetFunctionOptions() const { return func_opts.get(); }

 private:
  std::string name;
  std::variant<std::shared_ptr<arrow::Scalar>, SsaOperation> assignment;
  std::vector<std::string> arguments;
  std::shared_ptr<arrow::compute::FunctionOptions> func_opts;
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
