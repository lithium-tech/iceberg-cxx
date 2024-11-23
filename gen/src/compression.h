#pragma once

#include "parquet/platform.h"

namespace gen {
namespace compression {
constexpr std::string_view kUncompressedStr = "UNCOMPRESSED";
constexpr std::string_view kLz4Str = "LZ4";
constexpr std::string_view kZstdStr = "ZSTD";
constexpr std::string_view kSnappyStr = "SNAPPY";
}  // namespace compression

inline std::optional<std::string> ToString(parquet::Compression::type compression) {
  switch (compression) {
    case parquet::Compression::UNCOMPRESSED:
      return std::string(compression::kUncompressedStr);
    case parquet::Compression::LZ4:
      return std::string(compression::kLz4Str);
    case parquet::Compression::ZSTD:
      return std::string(compression::kZstdStr);
    case parquet::Compression::SNAPPY:
      return std::string(compression::kSnappyStr);
    default:
      return std::nullopt;
  }
  return std::nullopt;
}

inline std::optional<parquet::Compression::type> CompressionFromString(const std::string& compression_str) {
  if (compression_str == compression::kUncompressedStr) {
    return parquet::Compression::UNCOMPRESSED;
  } else if (compression_str == compression::kLz4Str) {
    return parquet::Compression::LZ4;
  } else if (compression_str == compression::kZstdStr) {
    return parquet::Compression::ZSTD;
  } else if (compression_str == compression::kSnappyStr) {
    return parquet::Compression::SNAPPY;
  } else {
    return std::nullopt;
  }
}

}  // namespace gen
