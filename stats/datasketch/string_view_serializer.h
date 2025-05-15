#pragma once

#include <string>
#include <vector>

#include "serde.hpp"

namespace stats {

class StringViewSerializer {
 public:
  explicit StringViewSerializer() {}

  size_t serialize(void* ptr, size_t capacity, const std::string_view* items, unsigned num) const {
    std::vector<std::string> uncompressed_data(num);
    for (unsigned i = 0; i < num; ++i) {
      uncompressed_data[i] = items[i];
    }
    return typed_serde_.serialize(ptr, capacity, uncompressed_data.data(), num);
  }

  size_t size_of_item(const std::string_view& item) const { return typed_serde_.size_of_item(std::string(item)); }

 private:
  datasketches::serde<std::string> typed_serde_;
};

}  // namespace stats
