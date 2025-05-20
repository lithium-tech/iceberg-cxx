#include "iceberg/tea_remote_catalog.h"

#include <ThriftHiveMetastore.h>

#include <stdexcept>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include <vector>

#include "arrow/api.h"
#include "iceberg/tea_scan.h"

namespace iceberg::ice_tea {

RemoteCatalog::RemoteCatalog(std::shared_ptr<IMetadataClient> client, std::shared_ptr<arrow::fs::S3FileSystem> s3fs)
    : impl_(std::move(client)), s3fs_(s3fs) {}

RemoteCatalog::~RemoteCatalog() = default;

bool RemoteCatalog::TableExists(const catalog::TableIdentifier& identifier) {
  return impl_->TableExists(identifier.db, identifier.name);
}

std::shared_ptr<Table> RemoteCatalog::LoadTable(const catalog::TableIdentifier& identifier) {
  auto location = impl_->GetMetadataLocation(identifier.db, identifier.name);
  if (properties_.empty()) {
    return std::make_shared<RemoteTable>(identifier, location, s3fs_);
  }

  if (!s3fs_) {
    if (!arrow::fs::IsS3Initialized()) {
      return {};
    }

    auto s3_opts = MakeS3Opts(properties_);
    auto res = arrow::fs::S3FileSystem::Make(s3_opts);
    if (!res.ok()) {
      return {};
    }
    s3fs_ = *res;
  }

  auto res = ReadFile(s3fs_, location);
  if (!res.ok()) {
    return {};
  }
  auto metadata = ice_tea::ReadTableMetadataV2(*res);
  if (!metadata) {
    return {};
  }
  return std::make_shared<RemoteTable>(identifier, location, metadata, s3fs_);
}

static inline std::string GetOpt(const std::map<std::string, std::string>& properties, const std::string& key) {
  auto it = properties.find(key);
  if (it != properties.end()) {
    return it->second;
  }
  return {};
}

arrow::fs::S3Options RemoteCatalog::MakeS3Opts(const std::map<std::string, std::string>& properties) {
  const std::string access_key = GetOpt(properties, OPT_ACCESS_KEY);
  const std::string secret_key = GetOpt(properties, OPT_SECRET_KEY);

  auto options = arrow::fs::S3Options::FromAccessKey(access_key, secret_key);
  options.endpoint_override = GetOpt(properties, OPT_ENDPOINT);
  options.scheme = GetOpt(properties, OPT_SCHEME);
  return options;
}

std::shared_ptr<Table> RemoteCatalog::CreateTable(const catalog::TableIdentifier& identifier, const Schema& schema,
                                                  std::shared_ptr<TableMetadataV2> table_metadata) {
  auto dir_path = "/" + identifier.db + "/" + identifier.name;
  auto status = s3fs_->CreateDir(dir_path);
  if (!status.ok()) {
    throw std::runtime_error("Could not create dir " + dir_path);
  }
  return std::make_shared<RemoteTable>(identifier, dir_path, table_metadata, s3fs_);
}

}  // namespace iceberg::ice_tea
