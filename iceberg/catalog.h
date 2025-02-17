#pragma once

#include <map>
#include <memory>
#include <string>

#include "iceberg/table.h"
#include "iceberg/table_metadata.h"

namespace iceberg::catalog {

using Namespace = std::string;

struct TableIdentifier {
  Namespace db;
  std::string name;

  bool operator==(const TableIdentifier& other) const = default;
};

struct HasherTableIdentifier {
  size_t operator()(const TableIdentifier& table_ident) const {
    static const std::hash<std::string> hasher;
    auto hash1 = hasher(table_ident.db);
    auto hash2 = hasher(table_ident.name);
    return hash1 & (hash1 ^ hash2);
  }
};

class Catalog {
 public:
  virtual ~Catalog() = default;

  virtual std::string Name() const = 0;
  virtual void Initialize(const std::string& name, std::map<std::string, std::string>& properties) = 0;
  virtual const std::map<std::string, std::string>& Properties() const = 0;

  virtual std::shared_ptr<Table> CreateTable(const TableIdentifier& identifier, const Schema& schema,
                                             std::shared_ptr<TableMetadataV2> table_metadata) = 0;
  // virtual bool DropTable(const TableIdentifier& identifier, bool purge) = 0;
  // virtual std::vector<TableIdentifier> ListTables(const Namespace& db) = 0;
  virtual std::shared_ptr<Table> LoadTable(const TableIdentifier& identifier) = 0;
  virtual bool TableExists(const TableIdentifier& identifier) = 0;
};

}  // namespace iceberg::catalog
