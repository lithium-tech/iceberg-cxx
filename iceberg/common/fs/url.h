#pragma once

#include <string_view>
#include <utility>
#include <vector>

namespace iceberg {

struct UrlComponents {
  using ParameterCollection = std::vector<std::pair<std::string_view, std::string_view>>;

  std::string_view schema;
  std::string_view location;
  std::string_view path;
  ParameterCollection params;
};

UrlComponents SplitUrl(std::string_view url);

}  // namespace iceberg
