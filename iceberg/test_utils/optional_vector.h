#pragma once

#include <optional>
#include <vector>

namespace iceberg {

template <typename T>
using OptionalVector = std::vector<std::optional<T>>;

}
