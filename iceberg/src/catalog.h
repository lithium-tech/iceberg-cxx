#pragma once

#include <memory>
#include <string>

#include "iceberg/src/table.h"

namespace iceberg::catalog {

using Namespace = std::string;

struct TableIdentifier {
  Namespace db;
  std::string name;
};

class Catalog {
 public:
  virtual ~Catalog() = default;

  // virtual std::string Name() const = 0;
  // virtual std::shared_ptr<Table> CreateTable​(const TableIdentifier& identifier, const Schema& schema) = 0;
  // virtual bool DropTable(const TableIdentifier& identifier, bool purge) = 0;
  // virtual std::vector<TableIdentifier> ListTables(const Namespace& db) = 0;
  virtual std::shared_ptr<Table> LoadTable(const TableIdentifier& identifier) = 0;
  // virtual bool TableExists​(const TableIdentifier& identifier) = 0;
};

}  // namespace iceberg::catalog
