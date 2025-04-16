#pragma once

#include <string>

namespace iceberg {

// "s3://some-path" is valid
// "file://some-path" is valid
// "some-path" is not valid
using FilePath = std::string;
using TableName = std::string;

}  // namespace iceberg
