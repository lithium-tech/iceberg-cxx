/*
Copypasted from
https://github.com/apache/arrow/blob/main/cpp/src/gandiva/precompiled/epoch_time_point.h
(milliseconds replaced with microseconds)
*/

#pragma once

#include "arrow/vendored/datetime/date.h"

namespace iceberg {

bool is_leap_year(int yy);
bool did_days_overflow(arrow_vendored::date::year_month_day ymd);
int last_possible_day_in_month(int month, int year);

// A point of time measured in micros since epoch.
class EpochTimePointMicros {
 public:
  explicit EpochTimePointMicros(std::chrono::microseconds micros_since_epoch) : tp_(micros_since_epoch) {}

  explicit EpochTimePointMicros(int64_t micros_since_epoch)
      : EpochTimePointMicros(std::chrono::microseconds(micros_since_epoch)) {}

  int TmYear() const { return static_cast<int>(YearMonthDay().year()) - 1900; }

  int TmMon() const { return static_cast<unsigned int>(YearMonthDay().month()) - 1; }

  int TmYday() const {
    auto to_days = arrow_vendored::date::floor<arrow_vendored::date::days>(tp_);
    auto first_day_in_year = arrow_vendored::date::sys_days{YearMonthDay().year() / arrow_vendored::date::jan / 1};
    return (to_days - first_day_in_year).count();
  }

  int TmMday() const { return static_cast<unsigned int>(YearMonthDay().day()); }

  int TmWday() const {
    auto to_days = arrow_vendored::date::floor<arrow_vendored::date::days>(tp_);
    return (arrow_vendored::date::weekday{to_days} -  // NOLINT
            arrow_vendored::date::Sunday)
        .count();
  }

  int TmHour() const { return static_cast<int>(TimeOfDay().hours().count()); }

  int TmMin() const { return static_cast<int>(TimeOfDay().minutes().count()); }

  int TmSec() const { return static_cast<int>(TimeOfDay().seconds().count()); }

  EpochTimePointMicros AddYears(int num_years) const {
    auto ymd = YearMonthDay() + arrow_vendored::date::years(num_years);
    return EpochTimePointMicros((arrow_vendored::date::sys_days{ymd} +  // NOLINT
                                 TimeOfDay().to_duration())
                                    .time_since_epoch());
  }

  EpochTimePointMicros AddMonths(int num_months) const {
    auto ymd = YearMonthDay() + arrow_vendored::date::months(num_months);

    EpochTimePointMicros tp = EpochTimePointMicros((arrow_vendored::date::sys_days{ymd} +  // NOLINT
                                                    TimeOfDay().to_duration())
                                                       .time_since_epoch());

    if (did_days_overflow(ymd)) {
      int days_to_offset =
          last_possible_day_in_month(static_cast<int>(ymd.year()), static_cast<unsigned int>(ymd.month())) -
          static_cast<unsigned int>(ymd.day());
      tp = tp.AddDays(days_to_offset);
    }
    return tp;
  }

  EpochTimePointMicros AddDays(int num_days) const {
    auto days_since_epoch = arrow_vendored::date::sys_days{YearMonthDay()} +  // NOLINT
                            arrow_vendored::date::days(num_days);
    return EpochTimePointMicros((days_since_epoch + TimeOfDay().to_duration()).time_since_epoch());
  }

  EpochTimePointMicros ClearTimeOfDay() const {
    return EpochTimePointMicros((tp_ - TimeOfDay().to_duration()).time_since_epoch());
  }

  bool operator==(const EpochTimePointMicros& other) const { return tp_ == other.tp_; }

  int64_t MicrosSinceEpoch() const { return tp_.time_since_epoch().count(); }

  arrow_vendored::date::time_of_day<std::chrono::microseconds> TimeOfDay() const {
    auto micros_since_midnight = tp_ - arrow_vendored::date::floor<arrow_vendored::date::days>(tp_);
    return arrow_vendored::date::time_of_day<std::chrono::microseconds>(micros_since_midnight);
  }

  arrow_vendored::date::year_month_day YearMonthDay() const {
    return arrow_vendored::date::year_month_day{
        arrow_vendored::date::floor<arrow_vendored::date::days>(tp_)};  // NOLINT
  }

 private:
  std::chrono::time_point<std::chrono::system_clock, std::chrono::microseconds> tp_;
};

}  // namespace iceberg
