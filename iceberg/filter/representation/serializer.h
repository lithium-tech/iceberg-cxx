#pragma once

#include <string>

#include "iceberg/filter/representation/node.h"

namespace iceberg::filter {

NodePtr StringToFilter(const std::string& row_filter);
std::string FilterToString(NodePtr node);

}  // namespace iceberg::filter
