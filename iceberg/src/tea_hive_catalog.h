#pragma once

#include <memory>
#include <string>

#include "iceberg/src/catalog.h"
#include "iceberg/src/table.h"

namespace iceberg::ice_tea {

class HiveClientImpl;

class HiveTable : public Table {
 public:
  HiveTable(const catalog::TableIdentifier& ident, const std::string& location) : ident_(ident), location_(location) {}

  const std::string& Location() const override { return location_; }
  const std::string& Name() const override { return ident_.name; }

 private:
  catalog::TableIdentifier ident_;
  std::string location_;
};

class HiveCatalog : public catalog::Catalog {
 public:
  HiveCatalog(const std::string& host, int port);
  virtual ~HiveCatalog();

  std::shared_ptr<Table> LoadTable(const catalog::TableIdentifier& identifier) override;

 private:
  std::unique_ptr<HiveClientImpl> impl_;
};

}  // namespace iceberg::ice_tea
