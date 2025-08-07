#pragma once

#include <cstdint>

namespace iceberg {

bool IsNullTimestamp(int64_t, bool is_valid);
bool IsNotNullTimestamp(int64_t, bool is_valid);
int64_t CastTimestamp(int64_t value);
int64_t CastTimestampFromTimestamptz(int64_t timestamp, int64_t shift);
int64_t CastTimestamptzFromTimestamp(int64_t timestamp, int64_t shift);
int32_t CastDateFromTimestamp(int64_t timestamp);
int32_t DateDiff(int32_t lhs, int32_t rhs);

bool LessThanDateTimestamp(int32_t date, int64_t timestamp);
bool LessThanOrEqualToDateTimestamp(int32_t date, int64_t timestamp);
bool GreaterThanDateTimestamp(int32_t date, int64_t timestamp);
bool GreaterThanOrEqualToDateTimestamp(int32_t date, int64_t timestamp);
bool EqualDateTimestamp(int32_t date, int64_t timestamp);
bool NotEqualDateTimestamp(int32_t date, int64_t timestamp);

bool LessThanTimestampDate(int64_t timestamp, int32_t date);
bool LessThanOrEqualToTimestampDate(int64_t timestamp, int32_t date);
bool GreaterThanTimestampDate(int64_t timestamp, int32_t date);

bool GreaterThanOrEqualToTimestampDate(int64_t timestamp, int32_t date);
bool EqualTimestampDate(int64_t timestamp, int32_t date);
bool NotEqualTimestampDate(int64_t timestamp, int32_t date);

double ExtractMillennium(int64_t timestamp_micros);
double ExtractCentury(int64_t timestamp_micros);
double ExtractDecade(int64_t timestamp_micros);
double ExtractYear(int64_t timestamp_micros);
double ExtractQuarter(int64_t timestamp_micros);
double ExtractMonth(int64_t timestamp_micros);
double ExtractWeek(int64_t timestamp_micros);
double ExtractDay(int64_t timestamp_micros);
double ExtractHour(int64_t timestamp_micros);
double ExtractMinute(int64_t timestamp_micros);
double ExtractSecond(int64_t timestamp_micros);
double ExtractMilliseconds(int64_t timestamp_micros);
double ExtractMicroseconds(int64_t timestamp_micros);

int64_t DateTruncMillennium(int64_t timestamp_micros);
int64_t DateTruncCentury(int64_t timestamp_micros);
int64_t DateTruncDecade(int64_t timestamp_micros);
int64_t DateTruncYear(int64_t timestamp_micros);
int64_t DateTruncQuarter(int64_t timestamp_micros);
int64_t DateTruncMonth(int64_t timestamp_micros);
int64_t DateTruncWeek(int64_t timestamp_micros);
int64_t DateTruncDay(int64_t timestamp_micros);
int64_t DateTruncHour(int64_t timestamp_micros);
int64_t DateTruncMinute(int64_t timestamp_micros);
int64_t DateTruncSecond(int64_t timestamp_micros);
int64_t DateTruncMilliseconds(int64_t timestamp_micros);

}  // namespace iceberg
