#include "iceberg/common/fs/url.h"

#include <string_view>

namespace iceberg {

namespace {
using std::literals::string_view_literals::operator""sv;

constexpr std::string_view kSchemaDelimiter = "://";
constexpr std::string_view::size_type kNPos = std::string_view::npos;

inline void ParseParam(std::string_view param, UrlComponents::ParameterCollection& collection) {
  if (auto pos = param.find('='); pos != kNPos) {
    collection.emplace_back(param.substr(0, pos), param.substr(pos + 1));
  } else {
    collection.emplace_back(param, std::string_view());
  }
}
}  // namespace

UrlComponents SplitUrl(std::string_view url) {
  UrlComponents result;
  if (auto pos = url.find(kSchemaDelimiter); pos != kNPos) {
    result.schema = url.substr(0, pos);
    url.remove_prefix(pos + kSchemaDelimiter.size());
  }
  if (auto pos = url.find_first_of("/?#"sv); pos != kNPos) {
    result.location = url.substr(0, pos);
    url.remove_prefix(pos);
  } else {
    result.location = url;
    return result;
  }
  if (auto pos = url.find_first_of("?#"sv); pos != kNPos) {
    result.path = url.substr(0, pos);
    url.remove_prefix(pos);
  } else {
    result.path = url;
    return result;
  }
  while (!url.empty() && url.front() != '#') {
    if (auto pos = url.find_first_of("?&#"sv); pos != kNPos) {
      switch (url[pos]) {
        case '?':
          url.remove_prefix(1);
          continue;
        case '&':
          ParseParam(url.substr(0, pos), result.params);
          url.remove_prefix(pos + 1);
          break;
        case '#':
          ParseParam(url.substr(0, pos), result.params);
          url.remove_prefix(pos);
          break;
      }
    } else {
      ParseParam(url, result.params);
      return result;
    }
  }
  return result;
}

}  // namespace iceberg
