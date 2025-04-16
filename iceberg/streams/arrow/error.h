#pragma once

#include <stdexcept>
#include <string>

namespace iceberg {

inline void Ensure(bool condition, const std::string& message) {
  if (!condition) {
    throw std::runtime_error(message);
  }
}

}  // namespace iceberg
