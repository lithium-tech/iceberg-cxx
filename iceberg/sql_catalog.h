#pragma once

#include <arrow/filesystem/filesystem.h>

#include <vector>

#include "iceberg/catalog.h"
#include "iceberg/table.h"
#include "iceberg/transaction.h"

namespace iceberg {

class SqlTable : public Table {
 public:
  SqlTable(const catalog::TableIdentifier& ident, const std::string& location,
           const std::vector<std::string>& file_pathes, std::shared_ptr<arrow::fs::FileSystem> fs)
      : ident_(ident), location_(location), file_pathes_(file_pathes), fs_(fs) {}
  SqlTable(const catalog::TableIdentifier& ident, const std::string& location,
           const std::vector<std::string>& file_pathes, std::shared_ptr<TableMetadataV2> table_metadata,
           std::shared_ptr<arrow::fs::FileSystem> fs)
      : ident_(ident), location_(location), file_pathes_(file_pathes), table_metadata_(table_metadata), fs_(fs) {}

  const std::string& Location() const override { return location_; }
  const std::string& Name() const override { return ident_.name; }

  std::shared_ptr<Schema> GetSchema() const override {
    if (!schema_ && table_metadata_) {
      schema_ = table_metadata_->GetCurrentSchema();
    }
    return schema_;
  }

  std::vector<std::string> GetFilePathes() const override { return file_pathes_; }

  void AppendTable(std::shared_ptr<arrow::Table> table) override {
    auto transaction = Transaction(this, table_metadata_, fs_);
    transaction.AppendTable(table);
    transaction.Commit();
  }

 private:
  void DoCommit(const std::vector<DataFile>& file_updates) override {
    for (const auto& elem : file_updates) {
      file_pathes_.push_back(elem.file_path);
    }
  }

  catalog::TableIdentifier ident_;
  std::string location_;
  std::vector<std::string> file_pathes_;
  std::shared_ptr<TableMetadataV2> table_metadata_;
  mutable std::shared_ptr<Schema> schema_;
  std::shared_ptr<arrow::fs::FileSystem> fs_;
};

class SqlCatalog : public catalog::Catalog {
 public:
  SqlCatalog(const std::string& root_dir, std::shared_ptr<arrow::fs::FileSystem> fs = {});

  std::string Name() const override { return name_; }

  void Initialize(const std::string& name, std::map<std::string, std::string>& properties) override {
    name_ = name;
    properties_ = properties;
  }

  const std::map<std::string, std::string>& Properties() const override { return properties_; }

  std::shared_ptr<Table> LoadTable(const catalog::TableIdentifier& identifier) override;
  bool TableExists(const catalog::TableIdentifier& identifier) override;

  std::shared_ptr<Table> CreateTable(const catalog::TableIdentifier& identifier, const Schema& schema,
                                     std::shared_ptr<TableMetadataV2> table_metadata = nullptr) override;

 private:
  std::string root_dir_;
  std::string name_;
  std::shared_ptr<arrow::fs::FileSystem> fs_;
  std::map<std::string, std::string> properties_;
};

}  // namespace iceberg
