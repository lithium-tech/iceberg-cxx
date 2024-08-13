#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

typedef struct ColumnStats {
  int64_t null_count;               // -1 <=> not set
  int64_t distinct_count;           // -1 <=> not set
  int64_t not_null_count;           // -1 <=> not set
  int64_t total_compressed_size;    // -1 <=> not set
  int64_t total_uncompressed_size;  // -1 <=> not set
} ColumnStats;

#ifdef __cplusplus
}
#endif
