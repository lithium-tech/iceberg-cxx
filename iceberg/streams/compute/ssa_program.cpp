#include "streams/compute/ssa_program.h"

#include <algorithm>

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <cstdint>
#include <memory>
#include <unordered_set>
#include <vector>

#include "streams/compute/projection.h"

namespace {

std::shared_ptr<arrow::BooleanArray> MakeFilter(const std::vector<bool>& bits) {
  arrow::BooleanBuilder builder;
  auto res = builder.Resize(bits.size());
  res = builder.AppendValues(bits);

  std::shared_ptr<arrow::BooleanArray> out;
  res = builder.Finish(&out);
  return out;
}

std::vector<bool> CombineFilters(std::vector<bool>&& f1, std::vector<bool>&& f2) {
  if (f1.empty()) {
    return f2;
  } else if (f2.empty()) {
    return f1;
  }

  for (size_t i = 0; i < f1.size(); ++i) {
    f1[i] = f1[i] && f2[i];
  }
  return f1;
}

}  // namespace

namespace iceberg::compute {

// TODO: kCastTimestamptz, kLower, kUpper, kLTrim, kRTrim, kBTrim, kExtractTimestamp, kDateTrunc, kDateDiff, kModuloWithChecks, kCbrt
const char* GetFunctionName(SsaOperation op) {
  switch (op) {
    case SsaOperation::kCastInt4:    // TODO: rename from PG-style to adequate kCastInt32
    case SsaOperation::kCastInt8:    // TODO: rename from PG-style to adequate kCastInt64
    case SsaOperation::kCastFloat8:  // TODO: rename from PG-style to adequate kCastFloat64
    case SsaOperation::kCastTimestamp:
    case SsaOperation::kCastDate:
      return "cast";

    case SsaOperation::kIsNull:
      return "is_null";

    case SsaOperation::kEqual:
      return "equal";
    case SsaOperation::kNotEqual:
      return "not_equal";
    case SsaOperation::kLessThan:
      return "less";
    case SsaOperation::kLessThanOrEqualTo:
      return "less_equal";
    case SsaOperation::kGreaterThan:
      return "greater";
    case SsaOperation::kGreaterThanOrEqualTo:
      return "greater_equal";

    case SsaOperation::kBitwiseNot:
      return "invert";
    case SsaOperation::kBitwiseAnd:
      return "and";
    case SsaOperation::kBitwiseOr:
      return "or";
    case SsaOperation::kXor:
      return "xor";

    case SsaOperation::kAddWithoutChecks:
      return "add";
    case SsaOperation::kSubtractWithoutChecks:
      return "subtract";
    case SsaOperation::kMultiplyWithoutChecks:
      return "multiply";
    case SsaOperation::kDivideWithoutChecks:
      return "divide";
    case SsaOperation::kAbsolute:
      return "abs";
    case SsaOperation::kNegative:
      return "negate";
    case SsaOperation::kAddWithChecks:
      return "add_checked";
    case SsaOperation::kSubtractWithChecks:
      return "subtract_checked";
    case SsaOperation::kMultiplyWithChecks:
      return "multiply_checked";
    case SsaOperation::kDivideWithChecks:
      return "divide_checked";

    case SsaOperation::kSqrt:
      return "sqrt";
    case SsaOperation::kExp:
      return "exp";
    case SsaOperation::kLog10:
      return "log10";

    // TODO: kILike, kNotLike, kNotILike, kLocate, kConcatenate, kSubstring
    case SsaOperation::kCharLength:
      return "binary_length";
    case SsaOperation::kLike:
      return "match_like";

    case SsaOperation::kSign:
      return "sign";
    case SsaOperation::kFloor:
      return "floor";
    case SsaOperation::kCeil:
      return "ceil";
    case SsaOperation::kRound:
      return "round";

    // No kCot
    case SsaOperation::kSin:
      return "sin";
    case SsaOperation::kCos:
      return "cos";
    case SsaOperation::kTan:
      return "tan";
    case SsaOperation::kAtan:
      return "atan";
    case SsaOperation::kAtan2:
      return "atan2";
    case SsaOperation::kSinh:
      return "sinh";
    case SsaOperation::kCosh:
      return "cosh";
    case SsaOperation::kTanh:
      return "tanh";
    case SsaOperation::kAsin:
      return "asin";
    case SsaOperation::kAcos:
      return "acos";

    default:
      break;
  }
  return "";
}

std::optional<SsaOperation> ValidateOperation(SsaOperation op, uint32_t argsSize) {
  switch (op) {
    case SsaOperation::kEqual:
    case SsaOperation::kNotEqual:
    case SsaOperation::kLessThan:
    case SsaOperation::kLessThanOrEqualTo:
    case SsaOperation::kGreaterThan:
    case SsaOperation::kGreaterThanOrEqualTo:
    case SsaOperation::kBitwiseAnd:
    case SsaOperation::kBitwiseOr:
    case SsaOperation::kXor:
    case SsaOperation::kAddWithChecks:
    case SsaOperation::kSubtractWithChecks:
    case SsaOperation::kMultiplyWithChecks:
    case SsaOperation::kDivideWithChecks:
    case SsaOperation::kAddWithoutChecks:
    case SsaOperation::kSubtractWithoutChecks:
    case SsaOperation::kMultiplyWithoutChecks:
    case SsaOperation::kDivideWithoutChecks:
    case SsaOperation::kAtan2:
      if (argsSize == 2) return op;
      break;

    case SsaOperation::kCastInt4:
    case SsaOperation::kCastInt8:
    case SsaOperation::kCastFloat8:
    case SsaOperation::kCastTimestamp:
    case SsaOperation::kCastDate:
    case SsaOperation::kIsNull:
    case SsaOperation::kSign:
    case SsaOperation::kCharLength:
    case SsaOperation::kBitwiseNot:
    case SsaOperation::kAbsolute:
    case SsaOperation::kNegative:
    case SsaOperation::kLike:
    case SsaOperation::kFloor:
    case SsaOperation::kCeil:
    case SsaOperation::kRound:
    case SsaOperation::kSin:
    case SsaOperation::kCos:
    case SsaOperation::kTan:
    case SsaOperation::kAtan:
    case SsaOperation::kSinh:
    case SsaOperation::kCosh:
    case SsaOperation::kTanh:
    case SsaOperation::kAsin:
    case SsaOperation::kAcos:
      if (argsSize == 1) return op;
      break;

    default:
      break;
  }
  return {};
}

namespace {

template <typename TOpId, typename TOptions>
arrow::Result<arrow::Datum> CallFunctionById(TOpId funcId, const std::vector<std::string>& args,
                                             const TOptions* funcOpts, const ProgramStep::DatumBatch& batch,
                                             arrow::compute::ExecContext* ctx) {
  std::vector<arrow::Datum> arguments;
  arguments.reserve(args.size());

  for (auto& col_name : args) {
    auto column = batch.GetColumnByName(col_name);
    if (!column.ok()) {
      return column.status();
    }
    arguments.push_back(*column);
  }

  std::string func_name = GetFunctionName(funcId);

  if (ctx && ctx->func_registry()->GetFunction(func_name).ok()) {
    return arrow::compute::CallFunction(func_name, arguments, funcOpts, ctx);
  }
  return arrow::compute::CallFunction(func_name, arguments, funcOpts);
}

arrow::Result<arrow::Datum> CallFunctionByAssign(const Assign& assign, const ProgramStep::DatumBatch& batch,
                                                 arrow::compute::ExecContext* ctx) {
  return CallFunctionById(assign.GetOperation(), assign.GetArguments(), assign.GetFunctionOptions(), batch, ctx);
}

}  // namespace

arrow::Status ProgramStep::DatumBatch::AddColumn(const std::string& name, arrow::Datum&& column) {
  if (schema->GetFieldIndex(name) != -1) return arrow::Status::Invalid("Trying to add duplicate column '" + name + "'");

  auto field = arrow::field(name, column.type());
  if (!field || !field->type()->Equals(column.type())) {
    return arrow::Status::Invalid("Cannot create field");
  }
  if (!column.is_scalar() && column.length() != rows) {
    return arrow::Status::Invalid("Wrong column length");
  }

  schema = *schema->AddField(schema->num_fields(), field);
  datums.emplace_back(column);
  return arrow::Status::OK();
}

arrow::Result<arrow::Datum> ProgramStep::DatumBatch::GetColumnByName(const std::string& name) const {
  auto i = schema->GetFieldIndex(name);
  if (i < 0) {
    return arrow::Status::Invalid("Column '" + name + "' is not found or duplicate");
  }
  return datums[i];
}

std::shared_ptr<arrow::RecordBatch> ProgramStep::DatumBatch::ToRecordBatch() const {
  std::vector<std::shared_ptr<arrow::Array>> columns;
  columns.reserve(datums.size());
  for (auto& col : datums) {
    if (col.is_scalar()) {
      columns.push_back(*arrow::MakeArrayFromScalar(*col.scalar(), rows));
    } else if (col.is_array()) {
      if (col.length() == -1) {
        return {};
      }
      columns.push_back(col.make_array());
    }
  }
  return arrow::RecordBatch::Make(schema, rows, columns);
}

std::shared_ptr<ProgramStep::DatumBatch> ProgramStep::DatumBatch::FromRecordBatch(
    std::shared_ptr<arrow::RecordBatch>& batch) {
  std::vector<arrow::Datum> datums;
  datums.reserve(batch->num_columns());
  for (int64_t i = 0; i < batch->num_columns(); ++i) {
    datums.push_back(arrow::Datum(batch->column(i)));
  }
  return std::make_shared<ProgramStep::DatumBatch>(
      ProgramStep::DatumBatch{.schema = std::make_shared<arrow::Schema>(*batch->schema()),
                              .datums = std::move(datums),
                              .rows = batch->num_rows()});
}

arrow::Status ProgramStep::ApplyAssignes(ProgramStep::DatumBatch& batch, arrow::compute::ExecContext* ctx) const {
  if (assignes.empty()) {
    return arrow::Status::OK();
  }

  batch.datums.reserve(batch.datums.size() + assignes.size());
  for (auto& assign : assignes) {
    if (batch.GetColumnByName(assign.GetName()).ok()) {
      return arrow::Status::Invalid("Assign to existing column '" + assign.GetName() + "'");
    }

    arrow::Datum column;
    if (assign.IsConstant()) {
      column = assign.GetConstant();
    } else {
      auto func_result = CallFunctionByAssign(assign, batch, ctx);
      if (!func_result.ok()) {
        return func_result.status();
      }
      column = *func_result;
    }
    auto status = batch.AddColumn(assign.GetName(), std::move(column));
    if (!status.ok()) {
      return status;
    }
  }
  // return batch->Validate();
  return arrow::Status::OK();
}

arrow::Status ProgramStep::ApplyFilters(DatumBatch& batch) const {
  if (filters.empty()) {
    return arrow::Status::OK();
  }

  std::vector<std::vector<bool>> filter_vecs;
  filter_vecs.reserve(filters.size());
  for (auto& col_name : filters) {
    auto column = batch.GetColumnByName(col_name);
    if (!column.ok()) {
      return column.status();
    }
    if (!column->is_array() || column->type() != arrow::boolean()) {
      return arrow::Status::Invalid("Column '" + col_name + "' is not a boolean array");
    }

    auto bool_column = std::static_pointer_cast<arrow::BooleanArray>(column->make_array());
    filter_vecs.push_back(std::vector<bool>(bool_column->length()));
    auto& bits = filter_vecs.back();
    for (size_t i = 0; i < bits.size(); ++i) {
      bits[i] = bool_column->Value(i);
    }
  }

  std::vector<bool> bits;
  for (auto& f : filter_vecs) {
    bits = CombineFilters(std::move(bits), std::move(f));
  }

  if (bits.size()) {
    auto filter = MakeFilter(bits);

    std::unordered_set<std::string_view> needed_columns;
    const bool all_columns = projection.empty();
    if (!all_columns) {
      needed_columns.insert(projection.begin(), projection.end());
    }

    for (int64_t i = 0; i < batch.schema->num_fields(); ++i) {
      const bool needed = (all_columns || needed_columns.contains(batch.schema->field(i)->name()));
      if (batch.datums[i].is_array() && needed) {
        auto res = arrow::compute::Filter(batch.datums[i].make_array(), filter);
        if (!res.ok()) {
          return res.status();
        }
        if ((*res).kind() != batch.datums[i].kind()) {
          return arrow::Status::Invalid("Unexpected filter result");
        }

        batch.datums[i] = *res;
      }
    }

    int new_rows = 0;
    for (int64_t i = 0; i < filter->length(); ++i) {
      new_rows += filter->Value(i);
    }
    batch.rows = new_rows;
  }
  return arrow::Status::OK();
}

arrow::Status ProgramStep::ApplyProjection(DatumBatch& batch) const {
  if (projection.empty()) return arrow::Status::OK();
  std::vector<std::shared_ptr<arrow::Field>> newFields;
  std::vector<arrow::Datum> newDatums;
  for (size_t i = 0; i < projection.size(); ++i) {
    int field_index = batch.schema->GetFieldIndex(projection[i]);
    if (field_index == -1) {
      return arrow::Status::Invalid("Could not find column " + projection[i] + " in record batch schema");
    }
    newFields.push_back(batch.schema->field(field_index));
    newDatums.push_back(batch.datums[field_index]);
  }
  batch.schema = std::make_shared<arrow::Schema>(newFields);
  batch.datums = std::move(newDatums);
  return arrow::Status::OK();
}

arrow::Status ProgramStep::ApplyProjection(std::shared_ptr<arrow::RecordBatch>& batch) const {
  if (projection.empty()) {
    return arrow::Status::OK();
  }

  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (auto& column : projection) {
    fields.push_back(batch->schema()->GetFieldByName(column));
    if (!fields.back()) {
      return arrow::Status::Invalid("Wrong projection column '" + column + "'");
    }
  }
  batch = Projection(batch, std::make_shared<arrow::Schema>(fields), false);
  return arrow::Status::OK();
}

arrow::Status ProgramStep::Apply(std::shared_ptr<arrow::RecordBatch>& batch, arrow::compute::ExecContext* ctx) const {
  auto rb = DatumBatch::FromRecordBatch(batch);

  auto status = ApplyAssignes(*rb, ctx);
  if (!status.ok()) {
    return status;
  }

  status = ApplyFilters(*rb);
  if (!status.ok()) {
    return status;
  }

  status = ApplyProjection(*rb);
  if (!status.ok()) {
    return status;
  }

  batch = (*rb).ToRecordBatch();
  if (!batch) {
    return arrow::Status::Invalid("Failed to create program result");
  }
  return arrow::Status::OK();
}

}  // namespace iceberg::compute
