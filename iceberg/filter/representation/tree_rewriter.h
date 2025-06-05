#pragma once

#include <memory>

#include "iceberg/filter/representation/node.h"

namespace iceberg::filter {

class TreeRewriter {
 public:
  static NodePtr ScalarOverArrayToLogical(std::shared_ptr<ScalarOverArrayFunctionNode> node);
};

}  // namespace iceberg::filter
