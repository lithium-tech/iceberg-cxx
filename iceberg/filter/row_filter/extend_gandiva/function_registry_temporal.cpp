#include "iceberg/filter/row_filter/extend_gandiva/function_registry_temporal.h"

#include <cstdint>
#include <string>

#include "arrow/type_fwd.h"
#include "gandiva/function_registry.h"
#include "gandiva/native_function.h"
#include "iceberg/filter/row_filter/extend_gandiva/arithmetic_ops.h"
#include "iceberg/filter/row_filter/extend_gandiva/temporal_ops.h"

namespace iceberg {

// // https://www.postgresql.org/docs/9.4/functions-datetime.html

arrow::Status RegisterTemporalFunctions() {
  using arrow::boolean;
  using arrow::date32;
  using arrow::float64;
  using arrow::int64;
  using arrow::timestamp;
  using arrow::TimeUnit::MICRO;
  using gandiva::DataTypeVector;
  using gandiva::kResultNullIfNull;
  using gandiva::NativeFunction;

  auto func_registry = gandiva::default_function_registry();

  ARROW_RETURN_NOT_OK(
      func_registry->Register(NativeFunction("AddOverflow", {}, DataTypeVector{timestamp(MICRO), int64()},
                                             timestamp(MICRO), kResultNullIfNull, "AddOverflow_timestampmicro_int64",
                                             NativeFunction::kNeedsContext | NativeFunction::kCanReturnErrors),
                              reinterpret_cast<void*>(AddOverflow<int64_t>)));

  ARROW_RETURN_NOT_OK(
      func_registry->Register(NativeFunction("castTIMESTAMP", {}, DataTypeVector{int64()}, timestamp(MICRO),
                                             kResultNullIfNull, "castTIMESTAMP_int64"),
                              reinterpret_cast<void*>(CastTimestamp)));

  ARROW_RETURN_NOT_OK(func_registry->Register(NativeFunction("isnull", {}, DataTypeVector{timestamp(MICRO)}, boolean(),
                                                             gandiva::kResultNullNever, "isnull_timestamp"),
                                              reinterpret_cast<void*>(IsNullTimestamp)));

  ARROW_RETURN_NOT_OK(
      func_registry->Register(NativeFunction("isnotnull", {}, DataTypeVector{timestamp(MICRO)}, boolean(),
                                             gandiva::kResultNullNever, "isnotnull_timestamp"),
                              reinterpret_cast<void*>(IsNotNullTimestamp)));

  ARROW_RETURN_NOT_OK(
      func_registry->Register(NativeFunction("castTIMESTAMP", {}, DataTypeVector{timestamp(MICRO, "UTC"), int64()},
                                             timestamp(MICRO), kResultNullIfNull, "castTIMESTAMP_int64_int64"),
                              reinterpret_cast<void*>(CastTimestampFromTimestamptz)));

  ARROW_RETURN_NOT_OK(
      func_registry->Register(NativeFunction("castTIMESTAMPTZ", {}, DataTypeVector{timestamp(MICRO), int64()},
                                             timestamp(MICRO, "UTC"), kResultNullIfNull, "castTIMESTAMPTZ_int64_int64"),
                              reinterpret_cast<void*>(CastTimestamptzFromTimestamp)));

  ARROW_RETURN_NOT_OK(func_registry->Register(NativeFunction("castDATE", {}, DataTypeVector{timestamp(MICRO)}, date32(),
                                                             kResultNullIfNull, "castDATE_timestamp[us, tz=UTC]"),
                                              reinterpret_cast<void*>(CastDateFromTimestamp)));

  ARROW_RETURN_NOT_OK(
      func_registry->Register(NativeFunction("castTIMESTAMPTZ", {}, DataTypeVector{int64()}, timestamp(MICRO, "UTC"),
                                             kResultNullIfNull, "castTIMESTAMPTZ_int64"),
                              reinterpret_cast<void*>(CastTimestamp)));

  ARROW_RETURN_NOT_OK(func_registry->Register(NativeFunction("DateDiff", {}, DataTypeVector{date32(), date32()},
                                                             arrow::int32(), kResultNullIfNull, "DateDiff_int32_int32"),
                                              reinterpret_cast<void*>(DateDiff)));

#define EXTRACT_FUNC_PTR(UNIT) reinterpret_cast<void*>(Extract##UNIT)

#define SIGNATURE_EXTRACT(UNIT)                                                                                      \
  NativeFunction("Extract" + std::string(#UNIT), {}, DataTypeVector{timestamp(MICRO)}, float64(), kResultNullIfNull, \
                 "Extract" + std::string(#UNIT) + "_timestamp[us]")

#define REGISTER_EXTRACT(UNIT)                                                                     \
  do {                                                                                             \
    ARROW_RETURN_NOT_OK(func_registry->Register(SIGNATURE_EXTRACT(UNIT), EXTRACT_FUNC_PTR(UNIT))); \
  } while (false)

  REGISTER_EXTRACT(Millennium);
  REGISTER_EXTRACT(Century);
  REGISTER_EXTRACT(Decade);
  REGISTER_EXTRACT(Year);
  REGISTER_EXTRACT(Quarter);
  REGISTER_EXTRACT(Month);
  REGISTER_EXTRACT(Week);
  REGISTER_EXTRACT(Day);
  REGISTER_EXTRACT(Hour);
  REGISTER_EXTRACT(Minute);
  REGISTER_EXTRACT(Second);
  REGISTER_EXTRACT(Milliseconds);
  REGISTER_EXTRACT(Microseconds);

#undef REGISTER_EXTRACT
#undef SIGNATURE_EXTRACT
#undef EXTRACT_FUNC_PTR

#define COMPARISON_TIMESTAMP_TIMESTAMP_FUNC_PTR(COMPARISON) reinterpret_cast<void*>(COMPARISON<int64_t>)

#define SIGNATURE_COMPARSION_TIMESTAMP_TIMESTAMP(COMPARISON)                                                        \
  NativeFunction(#COMPARISON, {}, DataTypeVector{timestamp(MICRO), timestamp(MICRO)}, boolean(), kResultNullIfNull, \
                 #COMPARISON + std::string("_timestamp[us]_timestamp[us]"))

#define REGISTER_COMPARISON_TIMESTAMP_TIMESTAMP(GDV_NAME, FUNC_NAME)                                  \
  do {                                                                                                \
    ARROW_RETURN_NOT_OK(func_registry->Register(SIGNATURE_COMPARSION_TIMESTAMP_TIMESTAMP(GDV_NAME),   \
                                                COMPARISON_TIMESTAMP_TIMESTAMP_FUNC_PTR(FUNC_NAME))); \
  } while (false)

  REGISTER_COMPARISON_TIMESTAMP_TIMESTAMP(less_than, LessThan);
  REGISTER_COMPARISON_TIMESTAMP_TIMESTAMP(less_than_or_equal_to, LessThanOrEqualTo);
  REGISTER_COMPARISON_TIMESTAMP_TIMESTAMP(greater_than, GreaterThan);
  REGISTER_COMPARISON_TIMESTAMP_TIMESTAMP(greater_than_or_equal_to, GreaterThanOrEqualTo);
  REGISTER_COMPARISON_TIMESTAMP_TIMESTAMP(equal, Equal);
  REGISTER_COMPARISON_TIMESTAMP_TIMESTAMP(not_equal, NotEqual);

#undef REGISTER_COMPARISON_TIMESTAMP_TIMESTAMP
#undef SIGNATURE_COMPARSION_TIMESTAMP_TIMESTAMP
#undef COMPARISON_TIMESTAMP_TIMESTAMP_FUNC_PTR

#define COMPARISON_TIMESTAMPTZ_TIMESTAMPTZ_FUNC_PTR(COMPARISON) reinterpret_cast<void*>(COMPARISON<int64_t>)

#define SIGNATURE_COMPARSION_TIMESTAMPTZ_TIMESTAMPTZ(COMPARISON)                                               \
  NativeFunction(#COMPARISON, {}, DataTypeVector{timestamp(MICRO, "UTC"), timestamp(MICRO, "UTC")}, boolean(), \
                 kResultNullIfNull, #COMPARISON + std::string("_timestamp[us, tz=UTC]_timestamp[us, tz=UTC]"))

#define REGISTER_COMPARISON_TIMESTAMPTZ_TIMESTAMPTZ(GDV_NAME, FUNC_NAME)                                  \
  do {                                                                                                    \
    ARROW_RETURN_NOT_OK(func_registry->Register(SIGNATURE_COMPARSION_TIMESTAMPTZ_TIMESTAMPTZ(GDV_NAME),   \
                                                COMPARISON_TIMESTAMPTZ_TIMESTAMPTZ_FUNC_PTR(FUNC_NAME))); \
  } while (false)

  REGISTER_COMPARISON_TIMESTAMPTZ_TIMESTAMPTZ(less_than, LessThan);
  REGISTER_COMPARISON_TIMESTAMPTZ_TIMESTAMPTZ(less_than_or_equal_to, LessThanOrEqualTo);
  REGISTER_COMPARISON_TIMESTAMPTZ_TIMESTAMPTZ(greater_than, GreaterThan);
  REGISTER_COMPARISON_TIMESTAMPTZ_TIMESTAMPTZ(greater_than_or_equal_to, GreaterThanOrEqualTo);
  REGISTER_COMPARISON_TIMESTAMPTZ_TIMESTAMPTZ(equal, Equal);
  REGISTER_COMPARISON_TIMESTAMPTZ_TIMESTAMPTZ(not_equal, NotEqual);

#undef REGISTER_COMPARISON_TIMESTAMP_TIMESTAMP
#undef SIGNATURE_COMPARSION_TIMESTAMP_TIMESTAMP
#undef COMPARISON_TIMESTAMP_TIMESTAMP_FUNC_PTR

#define COMPARISON_DATE_TIMESTAMP_FUNC_PTR(COMPARISON) reinterpret_cast<void*>(COMPARISON##DateTimestamp)

#define SIGNATURE_COMPARSION_DATE_TIMESTAMP(COMPARISON)                                                     \
  NativeFunction(#COMPARISON, {}, DataTypeVector{date32(), timestamp(MICRO)}, boolean(), kResultNullIfNull, \
                 #COMPARISON + std::string("_date32[day]_timestamp[us]"))

#define REGISTER_COMPARISON_DATE_TIMESTAMP(GDV_NAME, FUNC_NAME)                                  \
  do {                                                                                           \
    ARROW_RETURN_NOT_OK(func_registry->Register(SIGNATURE_COMPARSION_DATE_TIMESTAMP(GDV_NAME),   \
                                                COMPARISON_DATE_TIMESTAMP_FUNC_PTR(FUNC_NAME))); \
  } while (false)

  REGISTER_COMPARISON_DATE_TIMESTAMP(less_than, LessThan);
  REGISTER_COMPARISON_DATE_TIMESTAMP(less_than_or_equal_to, LessThanOrEqualTo);
  REGISTER_COMPARISON_DATE_TIMESTAMP(greater_than, GreaterThan);
  REGISTER_COMPARISON_DATE_TIMESTAMP(greater_than_or_equal_to, GreaterThanOrEqualTo);
  REGISTER_COMPARISON_DATE_TIMESTAMP(equal, Equal);
  REGISTER_COMPARISON_DATE_TIMESTAMP(not_equal, NotEqual);

#undef REGISTER_COMPARISON_DATE_TIMESTAMP
#undef SIGNATURE_COMPARSION_DATE_TIMESTAMP
#undef COMPARISON_DATE_TIMESTAMP_FUNC_PTR

#define COMPARISON_TIMESTAMP_DATE_FUNC_PTR(COMPARISON) reinterpret_cast<void*>(COMPARISON##TimestampDate)

#define SIGNATURE_COMPARSION_TIMESTAMP_DATE(COMPARISON)                                                     \
  NativeFunction(#COMPARISON, {}, DataTypeVector{timestamp(MICRO), date32()}, boolean(), kResultNullIfNull, \
                 #COMPARISON + std::string("_timestamp[us]_date32[day]"))

#define REGISTER_COMPARISON_TIMESTAMP_DATE(GDV_NAME, FUNC_NAME)                                  \
  do {                                                                                           \
    ARROW_RETURN_NOT_OK(func_registry->Register(SIGNATURE_COMPARSION_TIMESTAMP_DATE(GDV_NAME),   \
                                                COMPARISON_TIMESTAMP_DATE_FUNC_PTR(FUNC_NAME))); \
  } while (false)

  REGISTER_COMPARISON_TIMESTAMP_DATE(less_than, LessThan);
  REGISTER_COMPARISON_TIMESTAMP_DATE(less_than_or_equal_to, LessThanOrEqualTo);
  REGISTER_COMPARISON_TIMESTAMP_DATE(greater_than, GreaterThan);
  REGISTER_COMPARISON_TIMESTAMP_DATE(greater_than_or_equal_to, GreaterThanOrEqualTo);
  REGISTER_COMPARISON_TIMESTAMP_DATE(equal, Equal);
  REGISTER_COMPARISON_TIMESTAMP_DATE(not_equal, NotEqual);

#undef REGISTER_COMPARISON_TIMESTAMP_DATE
#undef SIGNATURE_COMPARSION_TIMESTAMP_DATE
#undef COMPARISON_TIMESTAMP_DATE_FUNC_PTR

#define DATE_TRUNC_FUNC_PTR(UNIT) reinterpret_cast<void*>(DateTrunc##UNIT)

#define SIGNATURE_DATE_TRUNC(UNIT)                                                                         \
  NativeFunction("DateTrunc" + std::string(#UNIT), {}, DataTypeVector{timestamp(MICRO)}, timestamp(MICRO), \
                 kResultNullIfNull, "DateTrunc" + std::string(#UNIT) + "_timestamp[us]")

#define REGISTER_DATE_TRUNC(UNIT)                                                                        \
  do {                                                                                                   \
    ARROW_RETURN_NOT_OK(func_registry->Register(SIGNATURE_DATE_TRUNC(UNIT), DATE_TRUNC_FUNC_PTR(UNIT))); \
  } while (false)

  REGISTER_DATE_TRUNC(Millennium);
  REGISTER_DATE_TRUNC(Century);
  REGISTER_DATE_TRUNC(Decade);
  REGISTER_DATE_TRUNC(Year);
  REGISTER_DATE_TRUNC(Quarter);
  REGISTER_DATE_TRUNC(Month);
  REGISTER_DATE_TRUNC(Week);
  REGISTER_DATE_TRUNC(Day);
  REGISTER_DATE_TRUNC(Hour);
  REGISTER_DATE_TRUNC(Minute);
  REGISTER_DATE_TRUNC(Second);
  REGISTER_DATE_TRUNC(Milliseconds);

#undef REGISTER_DATE_TRUNC
#undef SIGNATURE_DATE_TRUNC
#undef DATE_TRUNC

  return arrow::Status::OK();
}

}  // namespace iceberg
