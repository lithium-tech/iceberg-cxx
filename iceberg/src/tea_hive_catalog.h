#pragma once

#include <map>
#include <memory>
#include <string>

#include "arrow/filesystem/s3fs.h"
#include "iceberg/src/catalog.h"
#include "iceberg/src/table.h"
#include "iceberg/src/table_metadata.h"

namespace iceberg::ice_tea {

class HiveClientImpl;

class HiveTable : public Table {
 public:
  HiveTable(const catalog::TableIdentifier& ident, const std::string& location) : ident_(ident), location_(location) {}
  HiveTable(const catalog::TableIdentifier& ident, const std::string& location,
            std::shared_ptr<TableMetadataV2> table_metadata)
      : ident_(ident), location_(location), table_metadata_(table_metadata) {}

  const std::string& Location() const override { return location_; }
  const std::string& Name() const override { return ident_.name; }

  std::shared_ptr<Schema> GetSchema() const override {
    if (!schema_ && table_metadata_) {
      schema_ = table_metadata_->GetCurrentSchema();
    }
    return schema_;
  }

 private:
  catalog::TableIdentifier ident_;
  std::string location_;
  std::shared_ptr<TableMetadataV2> table_metadata_;
  mutable std::shared_ptr<Schema> schema_;
};

class HiveCatalog : public catalog::Catalog {
 public:
  static constexpr const char* OPT_ACCESS_KEY = "s3.access_key";
  static constexpr const char* OPT_SECRET_KEY = "s3.secret_key";
  static constexpr const char* OPT_ENDPOINT = "s3.endpoint_override";
  static constexpr const char* OPT_SCHEME = "s3.scheme";

  HiveCatalog(const std::string& host, int port, std::shared_ptr<arrow::fs::S3FileSystem> s3fs = {});
  virtual ~HiveCatalog();

  std::string Name() const override { return name_; }

  void Initialize(const std::string& name, std::map<std::string, std::string>& properties) override {
    name_ = name;
    properties_ = properties;
  }

  const std::map<std::string, std::string>& Properties() const override { return properties_; }

  std::shared_ptr<Table> LoadTable(const catalog::TableIdentifier& identifier) override;
  bool TableExists(const catalog::TableIdentifier& identifier) override;

 private:
  std::string name_ = "ice_tea";
  std::unique_ptr<HiveClientImpl> impl_;
  std::shared_ptr<arrow::fs::S3FileSystem> s3fs_;
  std::map<std::string, std::string> properties_;

  static arrow::fs::S3Options MakeS3Opts(const std::map<std::string, std::string>& properties);
};

}  // namespace iceberg::ice_tea
