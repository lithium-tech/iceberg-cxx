#pragma once

#include <string>

#include "gen/src/generators.h"

namespace gen::tpch::text {

using Text = std::string;

Text GenerateText(gen::RandomDevice& random_device);

}  // namespace gen::tpch::text
