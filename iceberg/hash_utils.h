#pragma once

#include <algorithm>
#include <cstdint>

#include "arrow/array.h"
#include "arrow/array/util.h"
#include "arrow/buffer.h"
#include "arrow/compare.h"
#include "arrow/pretty_print.h"
#include "arrow/scalar.h"
#include "arrow/type.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/decimal.h"
#include "arrow/util/formatting.h"
#include "arrow/util/hashing.h"
#include "arrow/util/logging.h"
#include "arrow/util/time.h"
#include "arrow/util/unreachable.h"
#include "arrow/util/utf8.h"
#include "arrow/util/value_parsing.h"
#include "arrow/visit_scalar_inline.h"

#ifdef ICECXX_USE_SMHASHER
#include "MurmurHash3.h"
#endif

namespace iceberg {

namespace {

template <typename T>
void PackLittleEndian(T value, std::vector<uint8_t>& buffer) {
  buffer.resize(sizeof(T));
  std::memcpy(buffer.data(), &value, sizeof(value));
}

}  // namespace

struct MurMurHasher {
  template <typename T>
  int operator()(const T& src) const {
    throw std::runtime_error("Unsupported type");
  }

  template <typename T>
  int operator()(const T& src) const requires(std::is_same_v<T, std::string>) {
#ifdef ICECXX_USE_SMHASHER
    int hash_result;
    MurmurHash3_x86_32(src.data(), src.size(), 0, &hash_result);
    return hash_result;
#else
    std::hash<T> hasher;
    return hasher(src);
#endif
  }

  template <typename T>
  int operator()(const T& value) const requires(std::is_same_v<T, int> || std::is_same_v<T, int64_t>) {
#ifdef ICECXX_USE_SMHASHER
    std::vector<uint8_t> buffer;
    PackLittleEndian(static_cast<int64_t>(value), buffer);
    int hash_result;
    MurmurHash3_x86_32(buffer.data(), buffer.size(), 0, &hash_result);
    return hash_result;
#else
    std::hash<T> hasher;
    return hasher(value);
#endif
  }

  template <typename T>
  int operator()(const T& value) const requires(std::is_same_v<T, uint64_t> || std::is_same_v<T, unsigned int>) {
#ifdef ICECXX_USE_SMHASHER
    std::vector<uint8_t> buffer;
    PackLittleEndian(static_cast<uint64_t>(value), buffer);
    int hash_result;
    MurmurHash3_x86_32(buffer.data(), buffer.size(), 0, &hash_result);
    return hash_result;
#else
    std::hash<T> hasher;
    return hasher(value);
#endif
  }

  template <typename T>
  int operator()(double value) const requires(std::is_same_v<T, float> || std::is_same_v<T, double>) {
#ifdef ICECXX_USE_SMHASHER
    std::vector<uint8_t> buffer;
    PackLittleEndian(value, buffer);
    int hash_result;
    MurmurHash3_x86_32(buffer.data(), buffer.size(), 0, &hash_result);
    return hash_result;
#else
    std::hash<T> hasher;
    return hasher(value);
#endif
  }
};

/* Source wrapper for hasher defined here https://github.com/apache/arrow/blob/main/cpp/src/arrow/scalar.cc
 */
struct MurMurScalarHashImpl {
  arrow::Status Visit(const arrow::NullScalar& s) { return arrow::Status::OK(); }

  template <typename T>
  arrow::Status Visit(const arrow::internal::PrimitiveScalar<T>& s) {
    return ValueHash(s);
  }

  arrow::Status Visit(const arrow::BaseBinaryScalar& s) { return BufferHash(*s.value); }

  template <typename T>
  arrow::Status Visit(const arrow::TemporalScalar<T>& s) {
    return ValueHash(s);
  }

  arrow::Status Visit(const arrow::DayTimeIntervalScalar& s) {
    return StdHash(s.value.days) & StdHash(s.value.milliseconds);
  }

  arrow::Status Visit(const arrow::MonthDayNanoIntervalScalar& s) {
    return StdHash(s.value.days) & StdHash(s.value.months) & StdHash(s.value.nanoseconds);
  }

  // arrow::Status Visit(const arrow::Decimal32Scalar& s) { return StdHash(s.value.value()); }

  // arrow::Status Visit(const arrow::Decimal64Scalar& s) { return StdHash(s.value.value()); }

  arrow::Status Visit(const arrow::Decimal128Scalar& s) {
    return StdHash(s.value.low_bits()) & StdHash(s.value.high_bits());
  }

  arrow::Status Visit(const arrow::Decimal256Scalar& s) {
    arrow::Status status = arrow::Status::OK();
    // endianness doesn't affect result
    for (uint64_t elem : s.value.native_endian_array()) {
      status &= StdHash(elem);
    }
    return status;
  }

  arrow::Status Visit(const arrow::BaseListScalar& s) { return ArrayHash(*s.value); }

  arrow::Status Visit(const arrow::StructScalar& s) {
    for (const auto& child : s.value) {
      AccumulateHashFrom(*child);
    }
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DictionaryScalar& s) {
    AccumulateHashFrom(*s.value.index);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::DenseUnionScalar& s) {
    // type_code is ignored when comparing for equality, so do not hash it either
    AccumulateHashFrom(*s.value);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::SparseUnionScalar& s) {
    // type_code is ignored when comparing for equality, so do not hash it either
    AccumulateHashFrom(*s.value[s.child_id]);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::RunEndEncodedScalar& s) {
    AccumulateHashFrom(*s.value);
    return arrow::Status::OK();
  }

  arrow::Status Visit(const arrow::ExtensionScalar& s) {
    AccumulateHashFrom(*s.value);
    return arrow::Status::OK();
  }

  template <typename T>
  arrow::Status StdHash(const T& t) {
    static MurMurHasher hash;
    hash_ ^= hash(t);
    return arrow::Status::OK();
  }

  template <typename S>
  arrow::Status ValueHash(const S& s) {
    return StdHash(s.value);
  }

  arrow::Status BufferHash(const arrow::Buffer& b) {
    hash_ ^= arrow::internal::ComputeStringHash<1>(b.data(), b.size());
    return arrow::Status::OK();
  }

  arrow::Status ArrayHash(const arrow::Array& a) { return ArrayHash(*a.data()); }

  arrow::Status ArrayHash(const arrow::ArraySpan& a, int64_t offset, int64_t length) {
    // Calculate null count within the range
    const auto* validity = a.buffers[0].data;
    int64_t null_count = 0;
    if (validity != NULLPTR) {
      if (offset == a.offset && length == a.length) {
        null_count = a.GetNullCount();
      } else {
        null_count = length - arrow::internal::CountSetBits(validity, offset, length);
      }
    }

    RETURN_NOT_OK(StdHash(length) & StdHash(null_count));
    if (null_count != 0) {
      // We can't visit values without unboxing the whole array, so only hash
      // the null bitmap for now. Only hash the null bitmap if the null count
      // is not 0 to ensure hash consistency.
      hash_ = arrow::internal::ComputeBitmapHash(validity, /*seed=*/hash_,
                                                 /*bits_offset=*/offset, /*num_bits=*/length);
    }

    // Hash the relevant child arrays for each type taking offset and length
    // from the parent array into account if necessary.
    switch (a.type->id()) {
      case arrow::Type::STRUCT:
        for (const auto& child : a.child_data) {
          RETURN_NOT_OK(ArrayHash(child, offset, length));
        }
        break;
        // TODO(GH-35830): Investigate what should be the correct behavior for
        // each nested type.
      default:
        // By default, just hash the arrays without considering
        // the offset and length of the parent.
        for (const auto& child : a.child_data) {
          RETURN_NOT_OK(ArrayHash(child));
        }
        break;
    }
    return arrow::Status::OK();
  }

  arrow::Status ArrayHash(const arrow::ArraySpan& a) { return ArrayHash(a, a.offset, a.length); }

  explicit MurMurScalarHashImpl(const arrow::Scalar& scalar) : hash_(scalar.type->Hash()) {
    AccumulateHashFrom(scalar);
  }

  void AccumulateHashFrom(const arrow::Scalar& scalar) {
    // Note we already injected the type in ScalarHashImpl::ScalarHashImpl
    if (scalar.is_valid) {
      DCHECK_OK(VisitScalarInline(scalar, this));
    }
  }

  int64_t hash_;
};

}  // namespace iceberg
