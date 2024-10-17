#pragma once

#include <arrow/status.h>

#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "arrow/array/array_binary.h"
#include "arrow/array/builder_binary.h"
#include "arrow/type_fwd.h"
#include "gen/src/batch.h"
#include "gen/src/generators.h"
#include "gen/src/list.h"
#include "gen/tpch/text.h"

namespace gen {

namespace tpch {

class AppendLeadingZerosGenerator : public WithArgsStringGenerator {
 public:
  AppendLeadingZerosGenerator(const std::string& arg_name, int32_t length)
      : WithArgsStringGenerator({std::make_shared<arrow::Field>(arg_name, arrow::utf8())}), length_(length) {}

  std::string GenerateValue(BatchPtr batch, uint64_t index) override {
    auto value = std::static_pointer_cast<arrow::StringArray>(batch->Column(0))->GetString(index);
    if (value.size() < length_) {
      // redundant allocation, but it's not a big deal
      return std::string(length_ - value.size(), '0') + value;
    }
    return value;
  }

 private:
  const uint32_t length_;
};

class PhoneGenerator : public WithArgsStringGenerator {
 public:
  explicit PhoneGenerator(std::string_view arg_name, RandomDevice& random_device)
      : WithArgsStringGenerator({std::make_shared<arrow::Field>(std::string(arg_name), arrow::int32())}),
        random_device_(random_device),
        generator_three_digits_(100, 999),
        generator_four_digits_(1000, 9999) {}

  std::string GenerateValue(BatchPtr record_batch, uint64_t row_index) override {
    int32_t country_code = std::static_pointer_cast<arrow::Int32Array>(record_batch->Column(0))->GetView(row_index);
    int32_t local_number1 = generator_three_digits_(random_device_);
    int32_t local_number2 = generator_three_digits_(random_device_);
    int32_t local_number3 = generator_four_digits_(random_device_);
    return std::to_string(country_code + 10) + "-" + std::to_string(local_number1) + "-" +
           std::to_string(local_number2) + "-" + std::to_string(local_number3);
  }

 private:
  RandomDevice& random_device_;
  UniformInt64Distribution generator_three_digits_;
  UniformInt64Distribution generator_four_digits_;
};

class VStringGenerator : public StringFromCharsetGenerator {
 public:
  VStringGenerator(uint64_t min_length, uint64_t max_length, RandomDevice& random_device)
      : StringFromCharsetGenerator(min_length, max_length, std::string(kCharacterSet), random_device) {}

 private:
  static constexpr std::string_view kCharacterSet = "0123456789abcdefghijklmnopqrstuvwxyz ABCDEFGHIJKLMNOPQRSTUVWXYZ,";
  static_assert(kCharacterSet.size() == 2 * 26 + 10 + 2);
};

// Append zeros to positions [sparse_keep, sparse_keep + sparse_bits)
class SparseKeyGenerator : public WithArgsInt64Generator {
 public:
  SparseKeyGenerator(const std::string& arg_name, int32_t sparse_keep, int32_t sparse_bits)
      : WithArgsInt64Generator({std::make_shared<arrow::Field>(arg_name, arrow::int32())}),
        sparse_keep_(sparse_keep),
        sparse_bits_(sparse_bits) {}

  int64_t GenerateValue(BatchPtr record_batch, uint64_t row_index) override {
    int32_t key = std::static_pointer_cast<arrow::Int32Array>(record_batch->Column(0))->GetView(row_index);
    int32_t low_bits = key & ((1 << sparse_keep_) - 1);
    key >>= sparse_keep_;
    key <<= sparse_bits_ + sparse_keep_;
    key |= low_bits;
    return key;
  }

 private:
  const int32_t sparse_keep_;
  const int32_t sparse_bits_;
};

class TextStringGenerator : public TrivialStringGenerator {
 public:
  TextStringGenerator(const text::Text& text, RandomDevice& random_device, int64_t min_length, int64_t max_length)
      : text_(text),
        random_device_(random_device),
        offset_distribution_(0, text.size() - max_length),
        length_distribution_(min_length, max_length) {}

  std::string GenerateValue() {
    int64_t string_length = length_distribution_(random_device_);
    int64_t offset = offset_distribution_(random_device_);
    return text_.substr(offset, string_length);
  }

 private:
  const text::Text& text_;
  RandomDevice& random_device_;
  UniformInt64Distribution offset_distribution_;
  UniformInt64Distribution length_distribution_;
};

namespace supplier {

class CommentGenerator : public TrivialStringGenerator {
 public:
  CommentGenerator(const text::Text& text, RandomDevice& random_device)
      : random_device_(random_device),
        text_string_generator_(text, random_device, 25, 100),
        comment_type_distribution_(0, 1999) {}

  std::string GenerateValue() {
    std::string text_string = text_string_generator_.GenerateValue();

    int64_t comment_type = comment_type_distribution_(random_device_);
    if (comment_type == kGoodCommentValue) {
      ReplacePart(text_string, kStringCustomer, kStringRecommends);
    } else if (comment_type == kBadCommentValue) {
      ReplacePart(text_string, kStringCustomer, kStringComplaints);
    }
    return text_string;
  }

 private:
  void ReplacePart(std::string& result, std::string_view first_part, std::string_view second_part) {
    UniformInt64Distribution first_split_position_distribution_(
        0, result.size() - first_part.size() - second_part.size() - 1);
    auto pos_first = first_split_position_distribution_(random_device_);
    memcpy(result.data() + pos_first, first_part.data(), first_part.size());

    UniformInt64Distribution second_split_position_distribution_(pos_first + first_part.size(),
                                                                 result.size() - second_part.size() - 1);
    auto pos_second = second_split_position_distribution_(random_device_);
    memcpy(result.data() + pos_second, second_part.data(), second_part.size());
  }

  static constexpr int64_t kGoodCommentValue = 0;
  static constexpr int64_t kBadCommentValue = 1;
  static constexpr std::string_view kStringCustomer = "Customer";
  static constexpr std::string_view kStringRecommends = "Recommends";
  static constexpr std::string_view kStringComplaints = "Complaints";

  RandomDevice& random_device_;
  TextStringGenerator text_string_generator_;
  UniformInt64Distribution comment_type_distribution_;
};

}  // namespace supplier

namespace part {

class RetailPriceGenerator : public WithArgsInt64Generator {
 public:
  explicit RetailPriceGenerator(std::string_view arg_name)
      : WithArgsInt64Generator({std::make_shared<arrow::Field>(std::string(arg_name), arrow::int32())}) {}

  int64_t GenerateValue(BatchPtr record_batch, uint64_t row_index) override {
    int32_t partkey = std::static_pointer_cast<arrow::Int32Array>(record_batch->Column(0))->GetView(row_index);
    int32_t price = 90000;
    price += (partkey / 10) % 20001;
    price += (partkey % 1000) * 100;
    return price;
  }
};

class NameGenerator : public TrivialStringGenerator {
 public:
  NameGenerator(const gen::List& list, RandomDevice& random_device) : word_generator_(list, random_device) {}

  std::string GenerateValue() override {
    std::vector<std::string> words;
    for (int32_t i = 0; i < kNumWords; ++i) {
      while (true) {
        auto new_word = word_generator_.GenerateValue();

        bool word_is_new = true;
        for (int32_t j = 0; j < i; ++j) {
          if (words[j] == new_word) {
            word_is_new = false;
            break;
          }
        }

        if (word_is_new) {
          words.emplace_back(std::move(new_word));
          break;
        }
      }
    }

    std::string result;
    for (int32_t i = 0; i < kNumWords; ++i) {
      if (i != 0) {
        result += " ";
      }
      result += std::move(words[i]);
    }

    return result;
  }

  static constexpr int32_t kNumWords = 5;

 private:
  gen::StringFromListGenerator word_generator_;
};

}  // namespace part

namespace partsupp {
class SuppkeyGenerator : public WithArgsInt32Generator {
 public:
  SuppkeyGenerator(std::string_view partkey_name, std::string_view supp_id_name, const int32_t scale_factor)
      : WithArgsInt32Generator({std::make_shared<arrow::Field>(std::string(partkey_name), arrow::int32()),
                                std::make_shared<arrow::Field>(std::string(supp_id_name), arrow::int32())}),
        s_(scale_factor * 10'000) {}

  int32_t GenerateValue(BatchPtr record_batch, uint64_t row_index) override {
    auto partkey = std::static_pointer_cast<arrow::Int32Array>(record_batch->Column(0))->GetView(row_index);
    auto corresponding_supplier =
        std::static_pointer_cast<arrow::Int32Array>(record_batch->Column(1))->GetView(row_index);

    return (partkey + (corresponding_supplier * ((s_ / 4) + (partkey - 1) / s_))) % s_ + 1;
  }

 private:
  const int64_t s_;
};

}  // namespace partsupp

namespace orders {

class CustomerkeyGenerator : public TrivialInt32Generator {
 public:
  CustomerkeyGenerator(int64_t min_value, int64_t max_value, RandomDevice& random_device)
      : random_device_(random_device), generator_(min_value, max_value) {}

  int32_t GenerateValue() {
    while (true) {
      auto result = generator_(random_device_);
      if (result % 3 != 0) {
        return result;
      }
    }
  }

 private:
  RandomDevice& random_device_;
  UniformInt64Distribution generator_;
};

class OrderstatusGenerator : public AggregatorGenerator<arrow::StringType> {
 public:
  OrderstatusGenerator(std::string_view replevels_name, std::string_view linestatus_name)
      : AggregatorGenerator<arrow::StringType>(
            {std::make_shared<arrow::Field>(std::string(replevels_name), arrow::int32()),
             std::make_shared<arrow::Field>(std::string(linestatus_name), arrow::utf8())}) {}

  arrow::Status HandleArray(BatchPtr args, int64_t from, int64_t to, std::vector<std::string>& result) override {
    auto linestatus_column = std::static_pointer_cast<arrow::StringArray>(args->Column(0));

    bool all_f = true;
    bool all_o = true;

    for (int k = from; k < to; ++k) {
      auto linestatus = linestatus_column->GetView(k);
      if (linestatus != "F") {
        all_f = false;
      }
      if (linestatus != "O") {
        all_o = false;
      }
    }

    if (all_f) {
      result.emplace_back("F");
    } else if (all_o) {
      result.emplace_back("O");
    } else {
      result.emplace_back("P");
    }

    return arrow::Status::OK();
  }
};

class TotalpriceGenerator : public AggregatorGenerator<arrow::Int64Type> {
 public:
  TotalpriceGenerator(std::string_view replevels_name, std::string_view extendprice_name, std::string_view tax_name,
                      std::string_view discount_name)
      : AggregatorGenerator<arrow::Int64Type>(
            {std::make_shared<arrow::Field>(std::string(replevels_name), arrow::int32()),
             std::make_shared<arrow::Field>(std::string(extendprice_name), arrow::int64()),
             std::make_shared<arrow::Field>(std::string(tax_name), arrow::int64()),
             std::make_shared<arrow::Field>(std::string(discount_name), arrow::int64())}) {}

  arrow::Status HandleArray(BatchPtr args, int64_t from, int64_t to, std::vector<int64_t>& result) override {
    auto extendprice_column = std::static_pointer_cast<arrow::Int64Array>(args->Column(0));
    auto tax_column = std::static_pointer_cast<arrow::Int64Array>(args->Column(1));
    auto discount_column = std::static_pointer_cast<arrow::Int64Array>(args->Column(2));

    int64_t total_result = 0;
    for (int32_t k = from; k < to; ++k) {
      const int64_t extendprice = extendprice_column->GetView(k);
      const auto tax = tax_column->GetView(k);
      const auto discount = discount_column->GetView(k);
      total_result += extendprice * (100 - discount) / 100 * (100 + tax) / 100;
    }
    result.emplace_back(total_result);

    return arrow::Status::OK();
  }
};

}  // namespace orders

namespace lineitem {

class ReturnflagGenerator : public WithArgsStringGenerator {
 public:
  ReturnflagGenerator(std::string_view receipt_name, int32_t current_date, RandomDevice& random_device)
      : WithArgsStringGenerator({std::make_shared<arrow::Field>(std::string(receipt_name), arrow::date32())}),
        current_date_(current_date),
        generator_(gen::List("flag", std::vector<std::string>{"R", "A"}), random_device) {}

  std::string GenerateValue(BatchPtr record_batch, uint64_t row_index) override {
    int32_t receiptdate = std::static_pointer_cast<arrow::Date32Array>(record_batch->Column(0))->GetView(row_index);

    if (receiptdate <= current_date_) {
      return generator_.GenerateValue();
    } else {
      return "N";
    }
  }

 private:
  const int32_t current_date_;
  StringFromListGenerator generator_;
};

class LinestatusGenerator : public WithArgsStringGenerator {
 public:
  LinestatusGenerator(std::string_view shipdate_name, int32_t current_date)
      : WithArgsStringGenerator({std::make_shared<arrow::Field>(std::string(shipdate_name), arrow::date32())}),
        current_date_(current_date) {}

  std::string GenerateValue(BatchPtr record_batch, uint64_t row_index) override {
    int32_t shipdate = std::static_pointer_cast<arrow::Date32Array>(record_batch->Column(0))->GetView(row_index);

    if (shipdate > current_date_) {
      return "O";
    } else {
      return "F";
    }
  }

 private:
  const int32_t current_date_;
};

}  // namespace lineitem

}  // namespace tpch

}  // namespace gen
