#pragma once

#include <map>
#include <memory>
#include <stdexcept>
#include <string>

#include "arrow/filesystem/s3fs.h"
#include "iceberg/catalog.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/transaction.h"

namespace iceberg::ice_tea {

struct IMetadataClient {
  virtual std::string GetMetadataLocation(const std::string& db_name, const std::string& table_name) = 0;
  virtual bool TableExists(const std::string& db_name, const std::string& table_name) = 0;

  virtual ~IMetadataClient() = default;
};

class RemoteTable : public Table {
 public:
  RemoteTable(const catalog::TableIdentifier& ident, const std::string& location,
              std::shared_ptr<arrow::fs::S3FileSystem> s3fs)
      : ident_(ident), location_(location), s3fs_(s3fs) {}
  RemoteTable(const catalog::TableIdentifier& ident, const std::string& location,
              std::shared_ptr<TableMetadataV2> table_metadata, std::shared_ptr<arrow::fs::S3FileSystem> s3fs)
      : ident_(ident), location_(location), table_metadata_(table_metadata), s3fs_(s3fs) {}

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
    auto transaction = Transaction(this, table_metadata_, s3fs_);
    transaction.AppendTable(table);
    transaction.Commit();
  }

 private:
  void DoCommit(const std::vector<DataFile>& file_updates) override {
    for (const auto& elem : file_updates) {
      file_pathes_.push_back(elem.file_path);
    }
  }

  std::vector<std::string> file_pathes_;
  catalog::TableIdentifier ident_;
  std::string location_;
  std::shared_ptr<TableMetadataV2> table_metadata_;
  mutable std::shared_ptr<Schema> schema_;
  std::shared_ptr<arrow::fs::S3FileSystem> s3fs_;
};

class RemoteCatalog : public catalog::Catalog {
 public:
  static constexpr const char* OPT_ACCESS_KEY = "s3.access_key";
  static constexpr const char* OPT_SECRET_KEY = "s3.secret_key";
  static constexpr const char* OPT_ENDPOINT = "s3.endpoint_override";
  static constexpr const char* OPT_SCHEME = "s3.scheme";

  RemoteCatalog(std::shared_ptr<IMetadataClient> client, std::shared_ptr<arrow::fs::S3FileSystem> s3fs = {});
  virtual ~RemoteCatalog();

  std::string Name() const override { return name_; }

  void Initialize(const std::string& name, std::map<std::string, std::string>& properties) override {
    name_ = name;
    properties_ = properties;
  }

  const std::map<std::string, std::string>& Properties() const override { return properties_; }

  std::shared_ptr<Table> LoadTable(const catalog::TableIdentifier& identifier) override;
  bool TableExists(const catalog::TableIdentifier& identifier) override;

  std::shared_ptr<Table> CreateTable(const catalog::TableIdentifier& identifier, const Schema& schema,
                                     std::shared_ptr<TableMetadataV2> table_metadata) override;

 private:
  std::string name_ = "ice_tea";
  std::shared_ptr<IMetadataClient> impl_;
  std::shared_ptr<arrow::fs::S3FileSystem> s3fs_;
  std::map<std::string, std::string> properties_;

  static arrow::fs::S3Options MakeS3Opts(const std::map<std::string, std::string>& properties);
};

}  // namespace iceberg::ice_tea
