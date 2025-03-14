#pragma once

#include <map>
#include <memory>
#include <stdexcept>
#include <string>

#include "arrow/filesystem/s3fs.h"
#include "iceberg/catalog.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/tea_remote_catalog.h"
#include "iceberg/transaction.h"
#include "rapidjson/document.h"

namespace iceberg::ice_tea {

class RESTClientImpl : public IMetadataClient {
 public:
  RESTClientImpl(const std::string& host, int port);

  std::string GetMetadataLocation(const std::string& db_name, const std::string& table_name) override;

  bool TableExists(const std::string& db_name, const std::string& table_name) override;

 private:
  std::optional<rapidjson::Document> GetTable(const std::string& db_name, const std::string& table_name);

  std::string base_url_;
};

using RESTTable = RemoteTable;

class RESTCatalog : public RemoteCatalog {
 public:
  RESTCatalog(const std::string& host, int port, std::shared_ptr<arrow::fs::S3FileSystem> s3fs = {});
};

}  // namespace iceberg::ice_tea
