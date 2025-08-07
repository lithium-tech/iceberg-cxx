#include "iceberg/filter/row_filter/extend_gandiva/function_registry_arithmetic.h"

#include <cstdint>
#include <string>

#include "gandiva/function_registry.h"
#include "gandiva/function_registry_common.h"
#include "gandiva/native_function.h"
#include "iceberg/filter/row_filter/extend_gandiva/arithmetic_ops.h"

namespace iceberg {

#define FUNC_PTR(NAME, BITS) reinterpret_cast<void*>(NAME<int##BITS##_t>)

arrow::Status RegisterArithmeticFunctions() {
  using arrow::int16;
  using arrow::int32;
  using arrow::int64;
  using gandiva::DataTypeVector;
  using gandiva::kResultNullIfNull;
  using gandiva::NativeFunction;

  auto func_registry = gandiva::default_function_registry();

#define REGISTER_BINARY_SYMMETRIC_INT_FUNCTION(NAME, BITS)                                                 \
  do {                                                                                                     \
    ARROW_RETURN_NOT_OK(func_registry->Register(BINARY_SYMMETRIC_UNSAFE_NULL_IF_NULL(NAME, {}, int##BITS), \
                                                reinterpret_cast<void*>(NAME<int##BITS##_t>)));            \
  } while (false)

#define REGISTER_BINARY_SYMMETRIC_INT_FUNCTIONS(NAME) \
  REGISTER_BINARY_SYMMETRIC_INT_FUNCTION(NAME, 16);   \
  REGISTER_BINARY_SYMMETRIC_INT_FUNCTION(NAME, 32);   \
  REGISTER_BINARY_SYMMETRIC_INT_FUNCTION(NAME, 64);

  REGISTER_BINARY_SYMMETRIC_INT_FUNCTIONS(AddOverflow);
  REGISTER_BINARY_SYMMETRIC_INT_FUNCTIONS(SubOverflow);
  REGISTER_BINARY_SYMMETRIC_INT_FUNCTIONS(MulOverflow);
  REGISTER_BINARY_SYMMETRIC_INT_FUNCTIONS(DivSafe);
  REGISTER_BINARY_SYMMETRIC_INT_FUNCTIONS(ModSafe);

#undef REGISTER_BINARY_SYMMETRIC_INT_FUNCTIONS

#undef REGISTER_BINARY_SYMMETRIC_INT_FUNCTION

  ARROW_RETURN_NOT_OK(
      func_registry->Register(NativeFunction("castINT", {}, gandiva::DataTypeVector{arrow::int16()}, arrow::int32(),
                                             gandiva::kResultNullIfNull, "castINT_int16"),
                              reinterpret_cast<void*>(Cast<int16_t, int32_t>)));

  ARROW_RETURN_NOT_OK(
      func_registry->Register(NativeFunction("castBIGINT", {}, gandiva::DataTypeVector{arrow::int16()}, arrow::int64(),
                                             gandiva::kResultNullIfNull, "castBIGINT_int16"),
                              reinterpret_cast<void*>(Cast<int16_t, int64_t>)));

  ARROW_RETURN_NOT_OK(
      func_registry->Register(NativeFunction("BankersRound", {}, gandiva::DataTypeVector{arrow::float64()},
                                             arrow::float64(), gandiva::kResultNullIfNull, "BankersRound_float64"),
                              reinterpret_cast<void*>(BankersRound)));

  return arrow::Status::OK();
}

}  // namespace iceberg
