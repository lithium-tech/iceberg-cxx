#include "iceberg/filter/row_filter/extend_gandiva/temporal_ops.h"

#include <cstdint>

#include "arrow/util/value_parsing.h"
#include "gtest/gtest.h"

namespace iceberg {

namespace {

inline int64_t TimestampAsSeconds(std::string_view s) {
  int64_t seconds = 0;
  ::arrow::internal::ParseTimestampStrptime(s.data(), s.length(), "%Y-%m-%d %H:%M:%S",
                                            /*ignore_time_in_day=*/false,
                                            /*allow_trailing_chars=*/false, ::arrow::TimeUnit::SECOND, &seconds);
  return seconds;
}

inline int64_t StringToTimestamp(const std::string& s) {
  if (s.find('.') == std::string::npos) {
    return TimestampAsSeconds(s) * 1'000'000;
  } else {
    auto dot_position = s.find('.');
    int64_t seconds = TimestampAsSeconds(s.substr(0, dot_position));
    std::string micros_as_str = s.substr(dot_position + 1);
    int64_t micros = std::stoi(micros_as_str);
    for (size_t i = micros_as_str.size(); i < 6; ++i) {
      micros *= 10;
    }
    return seconds * 1'000'000 + micros;
  }
}

TEST(TemporalTest, ExtractMillennium) {
  EXPECT_EQ(ExtractMillennium(StringToTimestamp("1970-01-01 00:00:00")), 2);
  EXPECT_EQ(ExtractMillennium(StringToTimestamp("1999-12-31 00:00:00")), 2);
  EXPECT_EQ(ExtractMillennium(StringToTimestamp("2000-12-31 00:00:00")), 2);
  EXPECT_EQ(ExtractMillennium(StringToTimestamp("2001-01-01 00:00:00")), 3);
  EXPECT_EQ(ExtractMillennium(StringToTimestamp("3000-12-31 00:00:00")), 3);
  EXPECT_EQ(ExtractMillennium(StringToTimestamp("3001-01-01 00:00:00")), 4);
}

TEST(TemporalTest, ExtractCentury) {
  EXPECT_EQ(ExtractCentury(StringToTimestamp("1970-01-01 00:00:00")), 20);
  EXPECT_EQ(ExtractCentury(StringToTimestamp("2000-12-31 00:00:00")), 20);
  EXPECT_EQ(ExtractCentury(StringToTimestamp("2001-01-01 00:00:00")), 21);
  EXPECT_EQ(ExtractCentury(StringToTimestamp("2100-12-31 00:00:00")), 21);
  EXPECT_EQ(ExtractCentury(StringToTimestamp("2101-01-01 00:00:00")), 22);
}

TEST(TemporalTest, ExtractDecade) {
  EXPECT_EQ(ExtractDecade(StringToTimestamp("1970-01-01 00:00:00")), 197);
  EXPECT_EQ(ExtractDecade(StringToTimestamp("1999-12-31 00:00:00")), 199);
  EXPECT_EQ(ExtractDecade(StringToTimestamp("2000-01-01 00:00:00")), 200);
  EXPECT_EQ(ExtractDecade(StringToTimestamp("2000-12-31 00:00:00")), 200);
  EXPECT_EQ(ExtractDecade(StringToTimestamp("2099-12-31 00:00:00")), 209);
  EXPECT_EQ(ExtractDecade(StringToTimestamp("2100-01-01 00:00:00")), 210);
}

TEST(TemporalTest, ExtractYear) {
  EXPECT_EQ(ExtractYear(StringToTimestamp("1970-01-01 00:00:00")), 1970);
  EXPECT_EQ(ExtractYear(StringToTimestamp("1999-12-31 00:00:00")), 1999);
  EXPECT_EQ(ExtractYear(StringToTimestamp("2000-01-01 00:00:00")), 2000);
  EXPECT_EQ(ExtractYear(StringToTimestamp("2000-12-31 00:00:00")), 2000);
  EXPECT_EQ(ExtractYear(StringToTimestamp("2099-12-31 00:00:00")), 2099);
  EXPECT_EQ(ExtractYear(StringToTimestamp("2100-01-01 00:00:00")), 2100);
}

TEST(TemporalTest, ExtractQuarter) {
  EXPECT_EQ(ExtractQuarter(StringToTimestamp("1999-12-31 00:00:00")), 4);
  EXPECT_EQ(ExtractQuarter(StringToTimestamp("2000-01-01 00:00:00")), 1);
  EXPECT_EQ(ExtractQuarter(StringToTimestamp("2000-03-31 00:00:00")), 1);
  EXPECT_EQ(ExtractQuarter(StringToTimestamp("2000-04-01 00:00:00")), 2);
  EXPECT_EQ(ExtractQuarter(StringToTimestamp("2000-06-30 00:00:00")), 2);
  EXPECT_EQ(ExtractQuarter(StringToTimestamp("2000-07-01 00:00:00")), 3);
  EXPECT_EQ(ExtractQuarter(StringToTimestamp("2000-09-30 00:00:00")), 3);
  EXPECT_EQ(ExtractQuarter(StringToTimestamp("2000-10-01 00:00:00")), 4);
  EXPECT_EQ(ExtractQuarter(StringToTimestamp("2000-12-31 00:00:00")), 4);
  EXPECT_EQ(ExtractQuarter(StringToTimestamp("2001-01-01 00:00:00")), 1);
}

TEST(TemporalTest, ExtractMonth) {
  EXPECT_EQ(ExtractMonth(StringToTimestamp("1999-12-31 00:00:00")), 12);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-01-01 00:00:00")), 1);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-01-31 00:00:00")), 1);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-02-01 00:00:00")), 2);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-02-29 00:00:00")), 2);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-03-01 00:00:00")), 3);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-03-31 00:00:00")), 3);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-04-01 00:00:00")), 4);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-04-30 00:00:00")), 4);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-05-01 00:00:00")), 5);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-05-31 00:00:00")), 5);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-06-01 00:00:00")), 6);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-06-30 00:00:00")), 6);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-07-01 00:00:00")), 7);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-07-31 00:00:00")), 7);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-08-01 00:00:00")), 8);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-08-31 00:00:00")), 8);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-09-01 00:00:00")), 9);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-09-30 00:00:00")), 9);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-10-01 00:00:00")), 10);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-10-31 00:00:00")), 10);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-11-01 00:00:00")), 11);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-11-30 00:00:00")), 11);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-12-01 00:00:00")), 12);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2000-12-31 00:00:00")), 12);
  EXPECT_EQ(ExtractMonth(StringToTimestamp("2001-01-01 00:00:00")), 1);
}

// https://github.com/apache/arrow/blob/b7d2f7ffca66c868bd2fce5b3749c6caa002a7f0/cpp/src/gandiva/precompiled/time_test.cc#L478
// test cases from
// http://www.staff.science.uu.nl/~gent0113/calendar/isocalendar.htm
TEST(TemporalTest, ExtractWeek) {
  std::vector<std::string> data;

  // A type
  // Jan 1, 2 and 3
  data.push_back("2006-01-01 10:10:10");
  data.push_back("52");
  data.push_back("2006-01-02 10:10:10");
  data.push_back("1");
  data.push_back("2006-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2006-04-24 10:10:10");
  data.push_back("17");
  data.push_back("2006-04-30 10:10:10");
  data.push_back("17");
  // Dec 29-31
  data.push_back("2006-12-29 10:10:10");
  data.push_back("52");
  data.push_back("2006-12-30 10:10:10");
  data.push_back("52");
  data.push_back("2006-12-31 10:10:10");
  data.push_back("52");
  // B(C) type
  // Jan 1, 2 and 3
  data.push_back("2011-01-01 10:10:10");
  data.push_back("52");
  data.push_back("2011-01-02 10:10:10");
  data.push_back("52");
  data.push_back("2011-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2011-07-18 10:10:10");
  data.push_back("29");
  data.push_back("2011-07-24 10:10:10");
  data.push_back("29");
  // Dec 29-31
  data.push_back("2011-12-29 10:10:10");
  data.push_back("52");
  data.push_back("2011-12-30 10:10:10");
  data.push_back("52");
  data.push_back("2011-12-31 10:10:10");
  data.push_back("52");
  // B(DC) type
  // Jan 1, 2 and 3
  data.push_back("2005-01-01 10:10:10");
  data.push_back("53");
  data.push_back("2005-01-02 10:10:10");
  data.push_back("53");
  data.push_back("2005-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2005-11-07 10:10:10");
  data.push_back("45");
  data.push_back("2005-11-13 10:10:10");
  data.push_back("45");
  // Dec 29-31
  data.push_back("2005-12-29 10:10:10");
  data.push_back("52");
  data.push_back("2005-12-30 10:10:10");
  data.push_back("52");
  data.push_back("2005-12-31 10:10:10");
  data.push_back("52");
  // C type
  // Jan 1, 2 and 3
  data.push_back("2010-01-01 10:10:10");
  data.push_back("53");
  data.push_back("2010-01-02 10:10:10");
  data.push_back("53");
  data.push_back("2010-01-03 10:10:10");
  data.push_back("53");
  // middle, Monday and Sunday
  data.push_back("2010-09-13 10:10:10");
  data.push_back("37");
  data.push_back("2010-09-19 10:10:10");
  data.push_back("37");
  // Dec 29-31
  data.push_back("2010-12-29 10:10:10");
  data.push_back("52");
  data.push_back("2010-12-30 10:10:10");
  data.push_back("52");
  data.push_back("2010-12-31 10:10:10");
  data.push_back("52");
  // D type
  // Jan 1, 2 and 3
  data.push_back("2037-01-01 10:10:10");
  data.push_back("1");
  data.push_back("2037-01-02 10:10:10");
  data.push_back("1");
  data.push_back("2037-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2037-08-17 10:10:10");
  data.push_back("34");
  data.push_back("2037-08-23 10:10:10");
  data.push_back("34");
  // Dec 29-31
  data.push_back("2037-12-29 10:10:10");
  data.push_back("53");
  data.push_back("2037-12-30 10:10:10");
  data.push_back("53");
  data.push_back("2037-12-31 10:10:10");
  data.push_back("53");
  // E type
  // Jan 1, 2 and 3
  data.push_back("2014-01-01 10:10:10");
  data.push_back("1");
  data.push_back("2014-01-02 10:10:10");
  data.push_back("1");
  data.push_back("2014-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2014-01-13 10:10:10");
  data.push_back("3");
  data.push_back("2014-01-19 10:10:10");
  data.push_back("3");
  // Dec 29-31
  data.push_back("2014-12-29 10:10:10");
  data.push_back("1");
  data.push_back("2014-12-30 10:10:10");
  data.push_back("1");
  data.push_back("2014-12-31 10:10:10");
  data.push_back("1");
  // F type
  // Jan 1, 2 and 3
  data.push_back("2019-01-01 10:10:10");
  data.push_back("1");
  data.push_back("2019-01-02 10:10:10");
  data.push_back("1");
  data.push_back("2019-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2019-02-11 10:10:10");
  data.push_back("7");
  data.push_back("2019-02-17 10:10:10");
  data.push_back("7");
  // Dec 29-31
  data.push_back("2019-12-29 10:10:10");
  data.push_back("52");
  data.push_back("2019-12-30 10:10:10");
  data.push_back("1");
  data.push_back("2019-12-31 10:10:10");
  data.push_back("1");
  // G type
  // Jan 1, 2 and 3
  data.push_back("2001-01-01 10:10:10");
  data.push_back("1");
  data.push_back("2001-01-02 10:10:10");
  data.push_back("1");
  data.push_back("2001-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2001-03-19 10:10:10");
  data.push_back("12");
  data.push_back("2001-03-25 10:10:10");
  data.push_back("12");
  // Dec 29-31
  data.push_back("2001-12-29 10:10:10");
  data.push_back("52");
  data.push_back("2001-12-30 10:10:10");
  data.push_back("52");
  data.push_back("2001-12-31 10:10:10");
  data.push_back("1");
  // AG type
  // Jan 1, 2 and 3
  data.push_back("2012-01-01 10:10:10");
  data.push_back("52");
  data.push_back("2012-01-02 10:10:10");
  data.push_back("1");
  data.push_back("2012-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2012-04-02 10:10:10");
  data.push_back("14");
  data.push_back("2012-04-08 10:10:10");
  data.push_back("14");
  // Dec 29-31
  data.push_back("2012-12-29 10:10:10");
  data.push_back("52");
  data.push_back("2012-12-30 10:10:10");
  data.push_back("52");
  data.push_back("2012-12-31 10:10:10");
  data.push_back("1");
  // BA type
  // Jan 1, 2 and 3
  data.push_back("2000-01-01 10:10:10");
  data.push_back("52");
  data.push_back("2000-01-02 10:10:10");
  data.push_back("52");
  data.push_back("2000-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2000-05-22 10:10:10");
  data.push_back("21");
  data.push_back("2000-05-28 10:10:10");
  data.push_back("21");
  // Dec 29-31
  data.push_back("2000-12-29 10:10:10");
  data.push_back("52");
  data.push_back("2000-12-30 10:10:10");
  data.push_back("52");
  data.push_back("2000-12-31 10:10:10");
  data.push_back("52");
  // CB type
  // Jan 1, 2 and 3
  data.push_back("2016-01-01 10:10:10");
  data.push_back("53");
  data.push_back("2016-01-02 10:10:10");
  data.push_back("53");
  data.push_back("2016-01-03 10:10:10");
  data.push_back("53");
  // middle, Monday and Sunday
  data.push_back("2016-06-20 10:10:10");
  data.push_back("25");
  data.push_back("2016-06-26 10:10:10");
  data.push_back("25");
  // Dec 29-31
  data.push_back("2016-12-29 10:10:10");
  data.push_back("52");
  data.push_back("2016-12-30 10:10:10");
  data.push_back("52");
  data.push_back("2016-12-31 10:10:10");
  data.push_back("52");
  // DC type
  // Jan 1, 2 and 3
  data.push_back("2004-01-01 10:10:10");
  data.push_back("1");
  data.push_back("2004-01-02 10:10:10");
  data.push_back("1");
  data.push_back("2004-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2004-07-19 10:10:10");
  data.push_back("30");
  data.push_back("2004-07-25 10:10:10");
  data.push_back("30");
  // Dec 29-31
  data.push_back("2004-12-29 10:10:10");
  data.push_back("53");
  data.push_back("2004-12-30 10:10:10");
  data.push_back("53");
  data.push_back("2004-12-31 10:10:10");
  data.push_back("53");
  // ED type
  // Jan 1, 2 and 3
  data.push_back("2020-01-01 10:10:10");
  data.push_back("1");
  data.push_back("2020-01-02 10:10:10");
  data.push_back("1");
  data.push_back("2020-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2020-08-17 10:10:10");
  data.push_back("34");
  data.push_back("2020-08-23 10:10:10");
  data.push_back("34");
  // Dec 29-31
  data.push_back("2020-12-29 10:10:10");
  data.push_back("53");
  data.push_back("2020-12-30 10:10:10");
  data.push_back("53");
  data.push_back("2020-12-31 10:10:10");
  data.push_back("53");
  // FE type
  // Jan 1, 2 and 3
  data.push_back("2008-01-01 10:10:10");
  data.push_back("1");
  data.push_back("2008-01-02 10:10:10");
  data.push_back("1");
  data.push_back("2008-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2008-09-15 10:10:10");
  data.push_back("38");
  data.push_back("2008-09-21 10:10:10");
  data.push_back("38");
  // Dec 29-31
  data.push_back("2008-12-29 10:10:10");
  data.push_back("1");
  data.push_back("2008-12-30 10:10:10");
  data.push_back("1");
  data.push_back("2008-12-31 10:10:10");
  data.push_back("1");
  // GF type
  // Jan 1, 2 and 3
  data.push_back("2024-01-01 10:10:10");
  data.push_back("1");
  data.push_back("2024-01-02 10:10:10");
  data.push_back("1");
  data.push_back("2024-01-03 10:10:10");
  data.push_back("1");
  // middle, Monday and Sunday
  data.push_back("2024-10-07 10:10:10");
  data.push_back("41");
  data.push_back("2024-10-13 10:10:10");
  data.push_back("41");
  // Dec 29-31
  data.push_back("2024-12-29 10:10:10");
  data.push_back("52");
  data.push_back("2024-12-30 10:10:10");
  data.push_back("1");
  data.push_back("2024-12-31 10:10:10");
  data.push_back("1");

  for (uint32_t i = 0; i < data.size(); i += 2) {
    int64_t ts = StringToTimestamp(data.at(i).c_str());
    int64_t exp = atol(data.at(i + 1).c_str());
    EXPECT_EQ(ExtractWeek(ts), exp);
  }
}

TEST(TemporalTest, ExtractDay) {
  EXPECT_EQ(ExtractDay(StringToTimestamp("1960-12-28 23:59:59")), 28);
  EXPECT_EQ(ExtractDay(StringToTimestamp("2000-01-05 00:00:00")), 5);
}

TEST(TemporalTest, ExtractHour) {
  EXPECT_EQ(ExtractHour(StringToTimestamp("2023-01-22 00:00:00")), 0);
  EXPECT_EQ(ExtractHour(StringToTimestamp("1960-07-15 01:59:59")), 1);
}

TEST(TemporalTest, ExtractMinute) {
  EXPECT_EQ(ExtractMinute(StringToTimestamp("1960-03-04 12:47:00")), 47);
  EXPECT_EQ(ExtractMinute(StringToTimestamp("2023-02-07 23:59:59.999999")), 59);
}

TEST(TemporalTest, ExtractSecond) {
  EXPECT_EQ(ExtractSecond(StringToTimestamp("1960-07-15 01:59:59.250000")), 59.25);
  EXPECT_EQ(ExtractSecond(StringToTimestamp("2023-03-04 12:11:47.312500")), 47.3125);
  EXPECT_EQ(ExtractSecond(StringToTimestamp("2023-02-07 13:38:23.000000")), 23);
}

TEST(TemporalTest, ExtractMilliseconds) {
  EXPECT_EQ(ExtractMilliseconds(StringToTimestamp("1960-07-15 01:59:59.250000")), 59250);
  EXPECT_EQ(ExtractMilliseconds(StringToTimestamp("2023-03-04 12:11:47.312500")), 47312.5);
  EXPECT_EQ(ExtractMilliseconds(StringToTimestamp("2023-02-07 13:38:23.000000")), 23000);
}

TEST(TemporalTest, ExtractMicroseconds) {
  EXPECT_EQ(ExtractMicroseconds(StringToTimestamp("1960-07-15 01:59:59.250000")), 59250000);
  EXPECT_EQ(ExtractMicroseconds(StringToTimestamp("2023-03-04 12:11:47.312500")), 47312500);
  EXPECT_EQ(ExtractMicroseconds(StringToTimestamp("2023-02-07 13:38:23.000000")), 23000000);
}

TEST(TemporalTest, DateTruncMillennium) {
  EXPECT_EQ(DateTruncMillennium(StringToTimestamp("1970-01-01 00:00:00")), StringToTimestamp("1001-01-01 00:00:00"));
  EXPECT_EQ(DateTruncMillennium(StringToTimestamp("1999-12-31 00:00:00")), StringToTimestamp("1001-01-01 00:00:00"));
  EXPECT_EQ(DateTruncMillennium(StringToTimestamp("2000-01-01 00:00:00")), StringToTimestamp("1001-01-01 00:00:00"));
  EXPECT_EQ(DateTruncMillennium(StringToTimestamp("2000-12-31 00:00:00")), StringToTimestamp("1001-01-01 00:00:00"));
  EXPECT_EQ(DateTruncMillennium(StringToTimestamp("2001-01-01 00:00:00")), StringToTimestamp("2001-01-01 00:00:00"));
  EXPECT_EQ(DateTruncMillennium(StringToTimestamp("3000-01-01 00:00:00")), StringToTimestamp("2001-01-01 00:00:00"));
  EXPECT_EQ(DateTruncMillennium(StringToTimestamp("3000-12-31 00:00:00")), StringToTimestamp("2001-01-01 00:00:00"));
  EXPECT_EQ(DateTruncMillennium(StringToTimestamp("3001-01-01 00:00:00")), StringToTimestamp("3001-01-01 00:00:00"));
}

TEST(TemporalTest, DateTruncCentury) {
  EXPECT_EQ(DateTruncCentury(StringToTimestamp("1970-01-01 00:00:00")), StringToTimestamp("1901-01-01 00:00:00"));
  EXPECT_EQ(DateTruncCentury(StringToTimestamp("2000-01-01 00:00:00")), StringToTimestamp("1901-01-01 00:00:00"));
  EXPECT_EQ(DateTruncCentury(StringToTimestamp("2000-12-31 00:00:00")), StringToTimestamp("1901-01-01 00:00:00"));
  EXPECT_EQ(DateTruncCentury(StringToTimestamp("2001-01-01 00:00:00")), StringToTimestamp("2001-01-01 00:00:00"));
  EXPECT_EQ(DateTruncCentury(StringToTimestamp("2100-12-31 00:00:00")), StringToTimestamp("2001-01-01 00:00:00"));
  EXPECT_EQ(DateTruncCentury(StringToTimestamp("2101-01-01 00:00:00")), StringToTimestamp("2101-01-01 00:00:00"));
}

TEST(TemporalTest, DateTruncDecade) {
  EXPECT_EQ(DateTruncDecade(StringToTimestamp("1970-01-01 00:00:00")), StringToTimestamp("1970-01-01 00:00:00"));
  EXPECT_EQ(DateTruncDecade(StringToTimestamp("1999-12-31 00:00:00")), StringToTimestamp("1990-01-01 00:00:00"));
  EXPECT_EQ(DateTruncDecade(StringToTimestamp("2000-01-01 00:00:00")), StringToTimestamp("2000-01-01 00:00:00"));
  EXPECT_EQ(DateTruncDecade(StringToTimestamp("2000-12-31 00:00:00")), StringToTimestamp("2000-01-01 00:00:00"));
  EXPECT_EQ(DateTruncDecade(StringToTimestamp("2099-12-31 00:00:00")), StringToTimestamp("2090-01-01 00:00:00"));
  EXPECT_EQ(DateTruncDecade(StringToTimestamp("2100-01-01 00:00:00")), StringToTimestamp("2100-01-01 00:00:00"));
}

TEST(TemporalTest, DateTruncYear) {
  EXPECT_EQ(DateTruncYear(StringToTimestamp("1970-01-01 00:00:00")), StringToTimestamp("1970-01-01 00:00:00"));
  EXPECT_EQ(DateTruncYear(StringToTimestamp("1999-12-31 00:00:00")), StringToTimestamp("1999-01-01 00:00:00"));
  EXPECT_EQ(DateTruncYear(StringToTimestamp("2000-01-01 00:00:00")), StringToTimestamp("2000-01-01 00:00:00"));
  EXPECT_EQ(DateTruncYear(StringToTimestamp("2000-12-31 00:00:00")), StringToTimestamp("2000-01-01 00:00:00"));
  EXPECT_EQ(DateTruncYear(StringToTimestamp("2099-12-31 00:00:00")), StringToTimestamp("2099-01-01 00:00:00"));
  EXPECT_EQ(DateTruncYear(StringToTimestamp("2100-01-01 00:00:00")), StringToTimestamp("2100-01-01 00:00:00"));
}

TEST(TemporalTest, DateTruncQuarter) {
  EXPECT_EQ(DateTruncQuarter(StringToTimestamp("1960-12-31 00:00:00")), StringToTimestamp("1960-10-01 00:00:00"));
  EXPECT_EQ(DateTruncQuarter(StringToTimestamp("2000-01-01 00:00:00")), StringToTimestamp("2000-01-01 00:00:00"));
  EXPECT_EQ(DateTruncQuarter(StringToTimestamp("2000-03-31 00:00:00")), StringToTimestamp("2000-01-01 00:00:00"));
  EXPECT_EQ(DateTruncQuarter(StringToTimestamp("2000-04-01 00:00:00")), StringToTimestamp("2000-04-01 00:00:00"));
  EXPECT_EQ(DateTruncQuarter(StringToTimestamp("2000-06-30 00:00:00")), StringToTimestamp("2000-04-01 00:00:00"));
  EXPECT_EQ(DateTruncQuarter(StringToTimestamp("2000-07-01 00:00:00")), StringToTimestamp("2000-07-01 00:00:00"));
  EXPECT_EQ(DateTruncQuarter(StringToTimestamp("2000-09-30 00:00:00")), StringToTimestamp("2000-07-01 00:00:00"));
  EXPECT_EQ(DateTruncQuarter(StringToTimestamp("2000-10-01 00:00:00")), StringToTimestamp("2000-10-01 00:00:00"));
  EXPECT_EQ(DateTruncQuarter(StringToTimestamp("2000-12-31 00:00:00")), StringToTimestamp("2000-10-01 00:00:00"));
  EXPECT_EQ(DateTruncQuarter(StringToTimestamp("2001-01-01 00:00:00")), StringToTimestamp("2001-01-01 00:00:00"));
}

TEST(TemporalTest, DateTruncMonth) {
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("1960-12-31 00:00:00")), StringToTimestamp("1960-12-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-01-01 00:00:00")), StringToTimestamp("2000-01-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-01-31 00:00:00")), StringToTimestamp("2000-01-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-02-01 00:00:00")), StringToTimestamp("2000-02-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-02-29 00:00:00")), StringToTimestamp("2000-02-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-03-01 00:00:00")), StringToTimestamp("2000-03-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-03-31 00:00:00")), StringToTimestamp("2000-03-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-04-01 00:00:00")), StringToTimestamp("2000-04-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-04-30 00:00:00")), StringToTimestamp("2000-04-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-05-01 00:00:00")), StringToTimestamp("2000-05-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-05-31 00:00:00")), StringToTimestamp("2000-05-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-06-01 00:00:00")), StringToTimestamp("2000-06-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-06-30 00:00:00")), StringToTimestamp("2000-06-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-07-01 00:00:00")), StringToTimestamp("2000-07-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-07-31 00:00:00")), StringToTimestamp("2000-07-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-08-01 00:00:00")), StringToTimestamp("2000-08-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-08-31 00:00:00")), StringToTimestamp("2000-08-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-09-01 00:00:00")), StringToTimestamp("2000-09-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-09-30 00:00:00")), StringToTimestamp("2000-09-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-10-01 00:00:00")), StringToTimestamp("2000-10-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-10-31 00:00:00")), StringToTimestamp("2000-10-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-11-01 00:00:00")), StringToTimestamp("2000-11-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-11-30 00:00:00")), StringToTimestamp("2000-11-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-12-01 00:00:00")), StringToTimestamp("2000-12-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2000-12-31 00:00:00")), StringToTimestamp("2000-12-01 00:00:00"));
  EXPECT_EQ(DateTruncMonth(StringToTimestamp("2001-01-01 00:00:00")), StringToTimestamp("2001-01-01 00:00:00"));
}

// https://github.com/apache/arrow/blob/b7d2f7ffca66c868bd2fce5b3749c6caa002a7f0/cpp/src/gandiva/precompiled/time_test.cc#L328
TEST(TemporalTest, DateTruncWeek) {
  EXPECT_EQ(DateTruncWeek(StringToTimestamp("2011-01-01 10:10:10")), StringToTimestamp("2010-12-27 00:00:00"));
  EXPECT_EQ(DateTruncWeek(StringToTimestamp("2011-01-02 10:10:10")), StringToTimestamp("2010-12-27 00:00:00"));
  EXPECT_EQ(DateTruncWeek(StringToTimestamp("2011-01-03 10:10:10")), StringToTimestamp("2011-01-03 00:00:00"));
  EXPECT_EQ(DateTruncWeek(StringToTimestamp("2011-01-04 10:10:10")), StringToTimestamp("2011-01-03 00:00:00"));
  EXPECT_EQ(DateTruncWeek(StringToTimestamp("2011-01-05 10:10:10")), StringToTimestamp("2011-01-03 00:00:00"));
  EXPECT_EQ(DateTruncWeek(StringToTimestamp("2011-01-06 10:10:10")), StringToTimestamp("2011-01-03 00:00:00"));
  EXPECT_EQ(DateTruncWeek(StringToTimestamp("2011-01-07 10:10:10")), StringToTimestamp("2011-01-03 00:00:00"));
  EXPECT_EQ(DateTruncWeek(StringToTimestamp("2011-01-08 10:10:10")), StringToTimestamp("2011-01-03 00:00:00"));
  EXPECT_EQ(DateTruncWeek(StringToTimestamp("2011-01-09 10:10:10")), StringToTimestamp("2011-01-03 00:00:00"));

  // truncate week for Feb in a leap year
  EXPECT_EQ(DateTruncWeek(StringToTimestamp("2000-02-28 10:10:10")), StringToTimestamp("2000-02-28 00:00:00"));
  EXPECT_EQ(DateTruncWeek(StringToTimestamp("2000-02-29 10:10:10")), StringToTimestamp("2000-02-28 00:00:00"));
  EXPECT_EQ(DateTruncWeek(StringToTimestamp("2000-03-01 10:10:10")), StringToTimestamp("2000-02-28 00:00:00"));
  EXPECT_EQ(DateTruncWeek(StringToTimestamp("2000-03-02 10:10:10")), StringToTimestamp("2000-02-28 00:00:00"));
  EXPECT_EQ(DateTruncWeek(StringToTimestamp("2000-03-03 10:10:10")), StringToTimestamp("2000-02-28 00:00:00"));
  EXPECT_EQ(DateTruncWeek(StringToTimestamp("2000-03-04 10:10:10")), StringToTimestamp("2000-02-28 00:00:00"));
  EXPECT_EQ(DateTruncWeek(StringToTimestamp("2000-03-05 10:10:10")), StringToTimestamp("2000-02-28 00:00:00"));
  EXPECT_EQ(DateTruncWeek(StringToTimestamp("2000-03-06 10:10:10")), StringToTimestamp("2000-03-06 00:00:00"));
}

TEST(TemporalTest, DateTruncDay) {
  EXPECT_EQ(DateTruncDay(StringToTimestamp("1960-12-28 23:59:59")), StringToTimestamp("1960-12-28 00:00:00"));
  EXPECT_EQ(DateTruncDay(StringToTimestamp("2000-01-05 00:01:03")), StringToTimestamp("2000-01-05 00:00:00"));
}

TEST(TemporalTest, DateTruncHour) {
  EXPECT_EQ(DateTruncHour(StringToTimestamp("2023-01-22 00:17:00")), StringToTimestamp("2023-01-22 00:00:00"));
  EXPECT_EQ(DateTruncHour(StringToTimestamp("1960-07-15 01:59:59")), StringToTimestamp("1960-07-15 01:00:00"));
}

TEST(TemporalTest, DateTruncMinute) {
  EXPECT_EQ(DateTruncMinute(StringToTimestamp("1960-03-04 12:47:01")), StringToTimestamp("1960-03-04 12:47:00"));
  EXPECT_EQ(DateTruncMinute(StringToTimestamp("2023-02-07 23:59:59.999999")), StringToTimestamp("2023-02-07 23:59:00"));
}

TEST(TemporalTest, DateTruncSecond) {
  EXPECT_EQ(DateTruncSecond(StringToTimestamp("1960-07-15 01:59:59.250000")), StringToTimestamp("1960-07-15 01:59:59"));
  EXPECT_EQ(DateTruncSecond(StringToTimestamp("2023-03-04 12:11:47.312500")), StringToTimestamp("2023-03-04 12:11:47"));
  EXPECT_EQ(DateTruncSecond(StringToTimestamp("2023-02-07 13:38:23.000000")), StringToTimestamp("2023-02-07 13:38:23"));
}

TEST(TemporalTest, DateTruncMilliseconds) {
  EXPECT_EQ(DateTruncMilliseconds(StringToTimestamp("1960-07-15 01:59:59.250000")),
            StringToTimestamp("1960-07-15 01:59:59.25"));
  EXPECT_EQ(DateTruncMilliseconds(StringToTimestamp("2023-03-04 12:11:47.312500")),
            StringToTimestamp("2023-03-04 12:11:47.312"));
  EXPECT_EQ(DateTruncMilliseconds(StringToTimestamp("2023-02-07 13:38:23.000000")),
            StringToTimestamp("2023-02-07 13:38:23"));
}

}  // namespace
}  // namespace iceberg
