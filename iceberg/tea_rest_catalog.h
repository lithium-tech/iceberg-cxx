#pragma once

#include <string>

#include "iceberg/tea_remote_catalog.h"

namespace iceberg::ice_tea {

class RESTCatalog : public RemoteCatalog {
 public:
  RESTCatalog(const std::string& rest_url, const std::string& warehouse_id);
};

}  // namespace iceberg::ice_tea
