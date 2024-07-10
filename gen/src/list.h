#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace gen {

struct List {
  struct StringWithWeight {
    std::string string;
    uint16_t weight;
  };

  List(const std::string& list_name, const std::vector<StringWithWeight>& elements)
      : list_name(list_name), elements(elements) {}

  List(const std::string& list_name, const std::vector<std::string>& strings) : list_name(list_name) {
    elements.reserve(strings.size());
    for (const auto& str : strings) {
      elements.emplace_back(StringWithWeight{.string = str, .weight = 1});
    }
  }

  std::string list_name;
  std::vector<StringWithWeight> elements;
};

}  // namespace gen
