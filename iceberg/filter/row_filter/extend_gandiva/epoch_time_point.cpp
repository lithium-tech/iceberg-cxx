#include "iceberg/filter/row_filter/extend_gandiva/epoch_time_point.h"

namespace iceberg {

// The first row is for non-leap years
static int days_in_a_month[2][12] = {{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
                                     {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}};

bool is_leap_year(int yy) {
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

bool did_days_overflow(arrow_vendored::date::year_month_day ymd) {
  int year = static_cast<int>(ymd.year());
  int month = static_cast<unsigned int>(ymd.month());
  int days = static_cast<unsigned int>(ymd.day());

  int matrix_index = is_leap_year(year) ? 1 : 0;

  return days > days_in_a_month[matrix_index][month - 1];
}

int last_possible_day_in_month(int year, int month) {
  int matrix_index = is_leap_year(year) ? 1 : 0;

  return days_in_a_month[matrix_index][month - 1];
}

}  // namespace iceberg
