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

namespace iceberg::ice_tea {

class HiveClientImpl;

class HiveClient : public IMetadataClient {
 public:
  HiveClient(const std::string& host, int port);

  std::string GetMetadataLocation(const std::string& db_name, const std::string& table_name) override;

 private:
  std::shared_ptr<HiveClientImpl> client_;
};

using HiveTable = RemoteTable;

class HiveCatalog : public RemoteCatalog {
 public:
  HiveCatalog(const std::string& host, int port, std::shared_ptr<arrow::fs::S3FileSystem> s3fs = {});
};

}  // namespace iceberg::ice_tea
