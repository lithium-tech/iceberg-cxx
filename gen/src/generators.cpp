#include "gen/src/generators.h"

#include <string>
#include <vector>

namespace gen {

StringFromListGenerator::StringFromListGenerator(const List& list, RandomDevice& random_device)
    : random_device_(random_device),
      values_([&list]() {
        std::vector<std::string> result;
        result.reserve(list.elements.size());
        for (const auto& elem : list.elements) {
          result.emplace_back(elem.string);
        }
        return result;
      }()),
      cumulative_weights_([&list]() {
        std::vector<int64_t> result;
        result.reserve(list.elements.size());
        int64_t cur_weight = 0;
        for (const auto& elem : list.elements) {
          cur_weight += elem.weight;
          result.emplace_back(cur_weight);
        }
        return result;
      }()),
      generator_(0, cumulative_weights_.back() - 1) {}

}  // namespace gen
