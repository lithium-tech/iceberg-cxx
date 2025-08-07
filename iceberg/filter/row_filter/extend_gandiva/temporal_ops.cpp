#include "iceberg/filter/row_filter/extend_gandiva/temporal_ops.h"

#include "iceberg/filter/row_filter/extend_gandiva/epoch_time_point.h"

namespace iceberg {

namespace {

constexpr int64_t kMicrosInMilli = 1'000;
constexpr int64_t kMicrosInSecond = 1'000 * kMicrosInMilli;
constexpr int64_t kMicrosInMinute = 60 * kMicrosInSecond;
constexpr int64_t kMicrosInHour = 60 * kMicrosInMinute;
constexpr int64_t kMicrosInDay = 24 * kMicrosInHour;

#define JAN1_WDAY(tp) ((tp.TmWday() - (tp.TmYday() % 7) + 7) % 7)

bool IsLeapYear(int yy) {
  if ((yy % 4) != 0) {
    // not divisible by 4
    return false;
  }

  // yy = 4x
  if ((yy % 400) == 0) {
    // yy = 400x
    return true;
  }

  // yy = 4x, return true if yy != 100x
  return ((yy % 100) != 0);
}

// Day belongs to current year
// Note that TmYday is 0 for Jan 1 (subtract 1 from day in the below examples)
//
// If Jan 1 is Mon, (TmYday) / 7 + 1 (Jan 1->WK1, Jan 8->WK2, etc)
// If Jan 1 is Tues, (TmYday + 1) / 7 + 1 (Jan 1->WK1, Jan 7->WK2, etc)
// If Jan 1 is Wed, (TmYday + 2) / 7 + 1
// If Jan 1 is Thu, (TmYday + 3) / 7 + 1
//
// If Jan 1 is Fri, Sat or Sun, the first few days belong to the previous year
// If Jan 1 is Fri, (TmYday - 3) / 7 + 1 (Jan 4->WK1, Jan 11->WK2)
// If Jan 1 is Sat, (TmYday - 2) / 7 + 1 (Jan 3->WK1, Jan 10->WK2)
// If Jan 1 is Sun, (TmYday - 1) / 7 + 1 (Jan 2->WK1, Jan 9->WK2)
int WeekOfCurrentYear(const EpochTimePointMicros& tp) {
  int jan1_wday = JAN1_WDAY(tp);
  switch (jan1_wday) {
    // Monday
    case 1:
    // Tuesday
    case 2:
    // Wednesday
    case 3:
    // Thursday
    case 4: {
      return (tp.TmYday() + jan1_wday - 1) / 7 + 1;
    }
    // Friday
    case 5:
    // Saturday
    case 6: {
      return (tp.TmYday() - (8 - jan1_wday)) / 7 + 1;
    }
    // Sunday
    case 0: {
      return (tp.TmYday() - 1) / 7 + 1;
    }
  }

  // cannot reach here
  // keep compiler happy
  return 0;
}

// Jan 1-3
// If Jan 1 is one of Mon, Tue, Wed, Thu - belongs to week of current year
// If Jan 1 is Fri/Sat/Sun - belongs to previous year
int GetJanWeekOfYear(const EpochTimePointMicros& tp) {
  int jan1_wday = JAN1_WDAY(tp);

  if ((jan1_wday >= 1) && (jan1_wday <= 4)) {
    // Jan 1-3 with the week belonging to this year
    return 1;
  }

  if (jan1_wday == 5) {
    // Jan 1 is a Fri
    // Jan 1-3 belong to previous year. Dec 31 of previous year same week # as
    // Jan 1-3 previous year is a leap year: Prev Jan 1 is a Wed. Jan 6th is Mon
    // Dec 31 - Jan 6 = 366 - 5 = 361
    // week from Jan 6 = (361 - 1) / 7 + 1 = 52
    // week # in previous year = 52 + 1 = 53
    //
    // previous year is not a leap year. Jan 1 is Thu. Jan 5th is Mon
    // Dec 31 - Jan 5 = 365 - 4 = 361
    // week from Jan 5 = (361 - 1) / 7 + 1 = 52
    // week # in previous year = 52 + 1 = 53
    return 53;
  }

  if (jan1_wday == 0) {
    // Jan 1 is a Sun
    if (tp.TmMday() > 1) {
      // Jan 2 and 3 belong to current year
      return 1;
    }

    // day belongs to previous year. Same as Dec 31
    // Same as the case where Jan 1 is a Fri, except that previous year
    // does not have an extra week
    // Hence, return 52
    return 52;
  }

  // Jan 1 is a Sat
  // Jan 1-2 belong to previous year
  if (tp.TmMday() == 3) {
    // Jan 3, return 1
    return 1;
  }

  // prev Jan 1 is leap year
  // prev Jan 1 is a Thu
  // return 53 (extra week)
  if (IsLeapYear(1900 + tp.TmYear() - 1)) {
    return 53;
  }

  // prev Jan 1 is not a leap year
  // prev Jan 1 is a Fri
  // return 52 (no extra week)
  return 52;
}

// Dec 29-31
int GetDecWeekOfYear(const EpochTimePointMicros& tp) {
  int next_jan1_wday = (tp.TmWday() + (31 - tp.TmMday()) + 1) % 7;

  if (next_jan1_wday == 4) {
    // next Jan 1 is a Thu
    // day belongs to week 1 of next year
    return 1;
  }

  if (next_jan1_wday == 3) {
    // next Jan 1 is a Wed
    // Dec 31 and 30 belong to next year - return 1
    if (tp.TmMday() != 29) {
      return 1;
    }

    // Dec 29 belongs to current year
    return WeekOfCurrentYear(tp);
  }

  if (next_jan1_wday == 2) {
    // next Jan 1 is a Tue
    // Dec 31 belongs to next year - return 1
    if (tp.TmMday() == 31) {
      return 1;
    }

    // Dec 29 and 30 belong to current year
    return WeekOfCurrentYear(tp);
  }

  // next Jan 1 is a Fri/Sat/Sun. No day from this year belongs to that week
  // next Jan 1 is a Mon. No day from this year belongs to that week
  return WeekOfCurrentYear(tp);
}

// Week of year is determined by ISO 8601 standard
// Take a look at: https://en.wikipedia.org/wiki/ISO_week_date

// Important points to note:
// Week starts with a Monday and ends with a Sunday
// A week can have some days in this year and some days in the previous/next
// year This is true for the first and last weeks

// The first week of the year should have at-least 4 days in the current year
// The last week of the year should have at-least 4 days in the current year

// A given day might belong to the first week of the next year - e.g Dec 29, 30
// and 31 A given day might belong to the last week of the previous year - e.g.
// Jan 1, 2 and 3

// Algorithm:
// If day belongs to week in current year, WeekOfCurrentYear

// If day is Jan 1-3, see GetJanWeekOfYear
// If day is Dec 29-21, see GetDecWeekOfYear

int64_t WeekOfYear(const EpochTimePointMicros& tp) {
  if (tp.TmYday() < 3) {
    // Jan 1-3
    return GetJanWeekOfYear(tp);
  }

  if ((tp.TmMon() == 11) && (tp.TmMday() >= 29)) {
    // Dec 29-31
    return GetDecWeekOfYear(tp);
  }

  return WeekOfCurrentYear(tp);
}

}  // namespace

bool IsNullTimestamp(int64_t, bool is_valid) { return !is_valid; }

bool IsNotNullTimestamp(int64_t, bool is_valid) { return is_valid; }

int64_t CastTimestamp(int64_t value) { return value; }

int64_t CastTimestampFromTimestamptz(int64_t timestamp, int64_t shift) { return timestamp + shift; }

int64_t CastTimestamptzFromTimestamp(int64_t timestamp, int64_t shift) { return timestamp + shift; }

int32_t CastDateFromTimestamp(int64_t timestamp) { return timestamp / kMicrosInDay; }

int32_t DateDiff(int32_t lhs, int32_t rhs) { return lhs - rhs; }

bool LessThanDateTimestamp(int32_t date, int64_t timestamp) { return date * kMicrosInDay < timestamp; }

bool LessThanOrEqualToDateTimestamp(int32_t date, int64_t timestamp) { return date * kMicrosInDay <= timestamp; }

bool GreaterThanDateTimestamp(int32_t date, int64_t timestamp) { return date * kMicrosInDay > timestamp; }

bool GreaterThanOrEqualToDateTimestamp(int32_t date, int64_t timestamp) { return date * kMicrosInDay >= timestamp; }

bool EqualDateTimestamp(int32_t date, int64_t timestamp) { return date * kMicrosInDay == timestamp; }

bool NotEqualDateTimestamp(int32_t date, int64_t timestamp) { return date * kMicrosInDay != timestamp; }

bool LessThanTimestampDate(int64_t timestamp, int32_t date) { return timestamp < date * kMicrosInDay; }

bool LessThanOrEqualToTimestampDate(int64_t timestamp, int32_t date) { return timestamp <= date * kMicrosInDay; }

bool GreaterThanTimestampDate(int64_t timestamp, int32_t date) { return timestamp > date * kMicrosInDay; }

bool GreaterThanOrEqualToTimestampDate(int64_t timestamp, int32_t date) { return timestamp >= date * kMicrosInDay; }

bool EqualTimestampDate(int64_t timestamp, int32_t date) { return timestamp == date * kMicrosInDay; }

bool NotEqualTimestampDate(int64_t timestamp, int32_t date) { return timestamp != date * kMicrosInDay; }

// https://github.com/postgres/postgres/blob/4bcf3521290af5317b6ff1f5ae98682e96a3013f/src/backend/utils/adt/timestamp.c#L4539
double ExtractMillennium(int64_t timestamp_micros) {
  EpochTimePointMicros tp(timestamp_micros);
  auto tm_year = static_cast<int32_t>(tp.YearMonthDay().year());
  if (tm_year > 0) {
    return (tm_year + 999) / 1000;
  } else {
    return -((999 - (tm_year - 1)) / 1000);
  }
}

// https://github.com/postgres/postgres/blob/4bcf3521290af5317b6ff1f5ae98682e96a3013f/src/backend/utils/adt/timestamp.c#L4524
double ExtractCentury(int64_t timestamp_micros) {
  EpochTimePointMicros tp(timestamp_micros);
  auto tm_year = static_cast<int32_t>(tp.YearMonthDay().year());
  if (tm_year > 0) {
    return (tm_year + 99) / 100;
  } else {
    return -((99 - (tm_year - 1)) / 100);
  }
}

// https://github.com/postgres/postgres/blob/4bcf3521290af5317b6ff1f5ae98682e96a3013f/src/backend/utils/adt/timestamp.c#L4511
double ExtractDecade(int64_t timestamp_micros) {
  EpochTimePointMicros tp(timestamp_micros);
  auto tm_year = static_cast<int32_t>(tp.YearMonthDay().year());
  if (tm_year > 0) {
    return tm_year / 10;
  } else {
    return -((8 - (tm_year - 1)) / 10);
  }
}

// https://github.com/postgres/postgres/blob/4bcf3521290af5317b6ff1f5ae98682e96a3013f/src/backend/utils/adt/timestamp.c#L4511
double ExtractYear(int64_t timestamp_micros) {
  EpochTimePointMicros tp(timestamp_micros);
  auto tm_year = static_cast<int32_t>(tp.YearMonthDay().year());
  if (tm_year > 0) {
    return tm_year;
  } else {
    return tm_year - 1;
  }
}

// https://github.com/postgres/postgres/blob/4bcf3521290af5317b6ff1f5ae98682e96a3013f/src/backend/utils/adt/timestamp.c#L4495
double ExtractQuarter(int64_t timestamp_micros) {
  EpochTimePointMicros tp(timestamp_micros);
  auto tm_mon = static_cast<uint32_t>(tp.YearMonthDay().month());
  return (tm_mon - 1) / 3 + 1;
}

// https://github.com/postgres/postgres/blob/4bcf3521290af5317b6ff1f5ae98682e96a3013f/src/backend/utils/adt/timestamp.c#L4491
double ExtractMonth(int64_t timestamp_micros) {
  EpochTimePointMicros tp(timestamp_micros);
  auto tm_mon = static_cast<uint32_t>(tp.YearMonthDay().month());
  return tm_mon;
}

// https://github.com/postgres/postgres/blob/4bcf3521290af5317b6ff1f5ae98682e96a3013f/src/backend/utils/adt/timestamp.c#L4499
double ExtractWeek(int64_t timestamp_micros) {
  EpochTimePointMicros tp(timestamp_micros);
  return WeekOfYear(tp);
}

// https://github.com/postgres/postgres/blob/4bcf3521290af5317b6ff1f5ae98682e96a3013f/src/backend/utils/adt/timestamp.c#L4487
double ExtractDay(int64_t timestamp_micros) {
  EpochTimePointMicros tp(timestamp_micros);
  return tp.TmMday();
}

// https://github.com/postgres/postgres/blob/4bcf3521290af5317b6ff1f5ae98682e96a3013f/src/backend/utils/adt/timestamp.c#L4483
double ExtractHour(int64_t timestamp_micros) {
  EpochTimePointMicros tp(timestamp_micros);
  return tp.TmHour();
}

// https://github.com/postgres/postgres/blob/4bcf3521290af5317b6ff1f5ae98682e96a3013f/src/backend/utils/adt/timestamp.c#L4479
double ExtractMinute(int64_t timestamp_micros) {
  EpochTimePointMicros tp(timestamp_micros);
  return tp.TmMin();
}

// https://github.com/postgres/postgres/blob/4bcf3521290af5317b6ff1f5ae98682e96a3013f/src/backend/utils/adt/timestamp.c#L4471
double ExtractSecond(int64_t timestamp_micros) {
  int64_t seconds_millis_micros_in_micros = timestamp_micros % kMicrosInMinute;
  if (seconds_millis_micros_in_micros < 0) {
    seconds_millis_micros_in_micros += kMicrosInMinute;
  }
  int64_t sec = seconds_millis_micros_in_micros / kMicrosInSecond;
  int64_t fsec = seconds_millis_micros_in_micros % kMicrosInSecond;
  return sec + fsec / 1'000'000.0;
}

// https://github.com/postgres/postgres/blob/4bcf3521290af5317b6ff1f5ae98682e96a3013f/src/backend/utils/adt/timestamp.c#L4463
double ExtractMilliseconds(int64_t timestamp_micros) {
  int64_t seconds_millis_micros_in_micros = timestamp_micros % kMicrosInMinute;
  if (seconds_millis_micros_in_micros < 0) {
    seconds_millis_micros_in_micros += kMicrosInMinute;
  }
  int64_t sec = seconds_millis_micros_in_micros / kMicrosInSecond;
  int64_t fsec = seconds_millis_micros_in_micros % kMicrosInSecond;
  return sec * 1'000.0 + fsec / 1'000.0;
}

// https://github.com/postgres/postgres/blob/4bcf3521290af5317b6ff1f5ae98682e96a3013f/src/backend/utils/adt/timestamp.c#L4455
double ExtractMicroseconds(int64_t timestamp_micros) {
  int64_t seconds_millis_micros_in_micros = timestamp_micros % kMicrosInMinute;
  if (seconds_millis_micros_in_micros < 0) {
    seconds_millis_micros_in_micros += kMicrosInMinute;
  }
  int64_t sec = seconds_millis_micros_in_micros / kMicrosInSecond;
  int64_t fsec = seconds_millis_micros_in_micros % kMicrosInSecond;
  return sec * 1'000'000.0 + fsec;
}

template <int years_in_unit, int off_by>
int64_t DateTruncYearUnits(int64_t timestamp_micros) {
  EpochTimePointMicros tp(timestamp_micros);
  int ndays_to_trunc = tp.TmMday() - 1;
  int nmonths_to_trunc = tp.TmMon();
  int year = 1900 + tp.TmYear();
  year = ((year - off_by) / years_in_unit) * years_in_unit + off_by;
  int nyears_to_trunc = tp.TmYear() - (year - 1900);
  return tp.AddDays(-ndays_to_trunc)
      .AddMonths(-nmonths_to_trunc)
      .AddYears(-nyears_to_trunc)
      .ClearTimeOfDay()
      .MicrosSinceEpoch();
}

template <int months_in_unit>
int64_t DateTruncMonthUnits(int64_t timestamp_micros) {
  EpochTimePointMicros tp(timestamp_micros);
  int ndays_to_trunc = tp.TmMday() - 1;
  int nmonths_to_trunc = tp.TmMon() - ((tp.TmMon() / months_in_unit) * months_in_unit);
  return tp.AddDays(-ndays_to_trunc).AddMonths(-nmonths_to_trunc).ClearTimeOfDay().MicrosSinceEpoch();
}

template <int64_t millis_in_unit>
int64_t DateTruncFixedUnit(int64_t timestamp_micros) {
  if (timestamp_micros < 0) [[unlikely]] {
    return (((timestamp_micros + 1) / millis_in_unit - 1) * millis_in_unit);
  }
  return ((timestamp_micros / millis_in_unit) * millis_in_unit);
}

int64_t DateTruncMillennium(int64_t timestamp_micros) { return DateTruncYearUnits<1000, 1>(timestamp_micros); }

int64_t DateTruncCentury(int64_t timestamp_micros) { return DateTruncYearUnits<100, 1>(timestamp_micros); }

int64_t DateTruncDecade(int64_t timestamp_micros) { return DateTruncYearUnits<10, 0>(timestamp_micros); }

int64_t DateTruncYear(int64_t timestamp_micros) { return DateTruncMonthUnits<12>(timestamp_micros); }

int64_t DateTruncQuarter(int64_t timestamp_micros) { return DateTruncMonthUnits<3>(timestamp_micros); }

int64_t DateTruncMonth(int64_t timestamp_micros) { return DateTruncMonthUnits<1>(timestamp_micros); }

int64_t DateTruncWeek(int64_t timestamp_micros) {
  EpochTimePointMicros tp(timestamp_micros);
  int ndays_to_trunc = 0;
  if (tp.TmWday() == 0) { /* Sunday */
    ndays_to_trunc = 6;
  } else { /* All other days */
    ndays_to_trunc = tp.TmWday() - 1;
  }
  return tp.AddDays(-ndays_to_trunc).ClearTimeOfDay().MicrosSinceEpoch();
}

int64_t DateTruncDay(int64_t timestamp_micros) { return DateTruncFixedUnit<kMicrosInDay>(timestamp_micros); }

int64_t DateTruncHour(int64_t timestamp_micros) { return DateTruncFixedUnit<kMicrosInHour>(timestamp_micros); }

int64_t DateTruncMinute(int64_t timestamp_micros) { return DateTruncFixedUnit<kMicrosInMinute>(timestamp_micros); }

int64_t DateTruncSecond(int64_t timestamp_micros) { return DateTruncFixedUnit<kMicrosInSecond>(timestamp_micros); }

int64_t DateTruncMilliseconds(int64_t timestamp_micros) { return DateTruncFixedUnit<kMicrosInMilli>(timestamp_micros); }

}  // namespace iceberg
