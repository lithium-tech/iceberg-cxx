#include "frequent_items_sketch.hpp"
#include "gtest/gtest.h"
#include "stats/datasketch/dictionary_serializer.h"

namespace stats {

TEST(DictionaryConvert, FrequentItems) {
  datasketches::frequent_items_sketch<int64_t> initial_sketch(8);
  {
    initial_sketch.update(0, 2);
    initial_sketch.update(1, 22);
    initial_sketch.update(2, 3);
    initial_sketch.update(2, 4);
    initial_sketch.update(2, 5);
  }

  std::vector<std::string> dictionary_values{{"aaa"}, {"bbb"}, {"ccc"}};
  std::span<std::string> span(dictionary_values.data(), dictionary_values.size());
  SpanValuesProvider<int64_t, std::string> provider(span);
  datasketches::frequent_items_sketch<std::string> sketch =
      ResolveDictionary<int64_t, std::string>(initial_sketch, provider);

  EXPECT_EQ(sketch.get_num_active_items(), 3);
  auto values = sketch.get_frequent_items(datasketches::frequent_items_error_type::NO_FALSE_NEGATIVES);

  ASSERT_EQ(values.size(), 3);
  EXPECT_EQ(values[0].get_item(), "bbb");
  EXPECT_EQ(values[0].get_lower_bound(), 22);
  EXPECT_EQ(values[0].get_upper_bound(), 22);

  EXPECT_EQ(values[1].get_item(), "ccc");
  EXPECT_EQ(values[1].get_lower_bound(), 12);
  EXPECT_EQ(values[1].get_upper_bound(), 12);

  EXPECT_EQ(values[2].get_item(), "aaa");
  EXPECT_EQ(values[2].get_lower_bound(), 2);
  EXPECT_EQ(values[2].get_upper_bound(), 2);
}

TEST(DictionaryConvert, Quantiles) {
  datasketches::quantiles_sketch<int64_t> initial_sketch(8);
  {
    for (int i = 0; i < 2; ++i) {
      initial_sketch.update(0);
    }
    for (int i = 0; i < 22; ++i) {
      initial_sketch.update(1);
    };
    for (int i = 0; i < 12; ++i) {
      initial_sketch.update(2);
    };
  }

  std::vector<std::string> dictionary_values{{"aaa"}, {"bbb"}, {"ccc"}};
  std::span<std::string> span(dictionary_values.data(), dictionary_values.size());
  SpanValuesProvider<int64_t, std::string> provider(span);

  datasketches::quantiles_sketch<std::string> sketch =
      ResolveDictionary<int64_t, std::string>(initial_sketch, provider);

  //   static_assert(std::is_arithmetic<std::string>::value);

  EXPECT_EQ(sketch.get_max_item(), "ccc");
  EXPECT_EQ(sketch.get_min_item(), "aaa");
}

}  // namespace stats
