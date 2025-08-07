#include "iceberg/filter/row_filter/extend_gandiva/register_functions.h"

#include "iceberg/filter/row_filter/extend_gandiva/function_registry_arithmetic.h"
#include "iceberg/filter/row_filter/extend_gandiva/function_registry_temporal.h"

namespace iceberg {

namespace {
enum class ExternalFunctionsRegistrationStatus { kTodo, kDone, kFailed };

arrow::Status RegisterFunctionsWithoutChecks() {
  ARROW_RETURN_NOT_OK(RegisterArithmeticFunctions());
  ARROW_RETURN_NOT_OK(RegisterTemporalFunctions());
  return arrow::Status::OK();
}

}  // namespace

arrow::Status RegisterFunctions() {
  static ExternalFunctionsRegistrationStatus registration_status = ExternalFunctionsRegistrationStatus::kTodo;
  if (registration_status == ExternalFunctionsRegistrationStatus::kTodo) {
    auto status = RegisterFunctionsWithoutChecks();
    if (!status.ok()) {
      registration_status = ExternalFunctionsRegistrationStatus::kFailed;
      return status;
    }
    registration_status = ExternalFunctionsRegistrationStatus::kDone;
  } else if (registration_status == ExternalFunctionsRegistrationStatus::kFailed) {
    return arrow::Status::ExecutionError("RegisterFunctions: failed");
  }

  return arrow::Status::OK();
}

}  // namespace iceberg
