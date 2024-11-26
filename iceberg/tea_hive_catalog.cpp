#include "iceberg/tea_hive_catalog.h"

#include <ThriftHiveMetastore.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include <stdexcept>
#include <vector>

#include "arrow/api.h"
#include "iceberg/tea_scan.h"

namespace iceberg::ice_tea {

class HiveClientImpl {
 public:
  HiveClientImpl(const std::string& host, int port)
      : socket_(new apache::thrift::transport::TSocket(host, port)),
        transport_(new apache::thrift::transport::TBufferedTransport(socket_)),
        protocol_(new apache::thrift::protocol::TBinaryProtocol(transport_)),
        client_(protocol_) {
    transport_->open();
  }

  ~HiveClientImpl() { transport_->close(); }

  Apache::Hadoop::Hive::Table GetTable(const std::string& db_name, const std::string& table_name) {
    Apache::Hadoop::Hive::Table table;
    client_.get_table(table, db_name, table_name);
    return table;
  }

  std::string GetMetadataLocation(const std::string& db_name, const std::string& table_name) {
    Apache::Hadoop::Hive::Table table = GetTable(db_name, table_name);
    if (table.tableName.empty()) {
      throw std::runtime_error("No table '" + table_name + "'");
    }
    if (auto it = table.parameters.find("metadata_location"); it == table.parameters.end()) {
      throw std::runtime_error("Table '" + table_name + "' has no metadata_location");
    } else {
      return it->second;
    }
  }

 private:
  std::shared_ptr<apache::thrift::transport::TTransport> socket_;
  std::shared_ptr<apache::thrift::transport::TTransport> transport_;
  std::shared_ptr<apache::thrift::protocol::TBinaryProtocol> protocol_;
  Apache::Hadoop::Hive::ThriftHiveMetastoreClient client_;
};

HiveCatalog::HiveCatalog(const std::string& host, int port, std::shared_ptr<arrow::fs::S3FileSystem> s3fs)
    : impl_(std::make_unique<HiveClientImpl>(host, port)), s3fs_(s3fs) {}
HiveCatalog::~HiveCatalog() = default;

bool HiveCatalog::TableExists(const catalog::TableIdentifier& identifier) {
  Apache::Hadoop::Hive::Table table = impl_->GetTable(identifier.db, identifier.name);
  return !table.tableName.empty();
}

std::shared_ptr<Table> HiveCatalog::LoadTable(const catalog::TableIdentifier& identifier) {
  auto location = impl_->GetMetadataLocation(identifier.db, identifier.name);
  if (properties_.empty()) {
    return std::make_shared<HiveTable>(identifier, location, s3fs_);
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
  return std::make_shared<HiveTable>(identifier, location, metadata, s3fs_);
}

static inline std::string GetOpt(const std::map<std::string, std::string>& properties, const std::string& key) {
  auto it = properties.find(key);
  if (it != properties.end()) {
    return it->second;
  }
  return {};
}

arrow::fs::S3Options HiveCatalog::MakeS3Opts(const std::map<std::string, std::string>& properties) {
  const std::string access_key = GetOpt(properties, OPT_ACCESS_KEY);
  const std::string secret_key = GetOpt(properties, OPT_SECRET_KEY);

  auto options = arrow::fs::S3Options::FromAccessKey(access_key, secret_key);
  options.endpoint_override = GetOpt(properties, OPT_ENDPOINT);
  options.scheme = GetOpt(properties, OPT_SCHEME);
  return options;
}

std::shared_ptr<Table> HiveCatalog::CreateTable(const catalog::TableIdentifier& identifier, const Schema& schema,
                                                std::shared_ptr<TableMetadataV2> table_metadata) {
  auto dir_path = "/" + identifier.db + "/" + identifier.name;
  auto status = s3fs_->CreateDir(dir_path);
  if (!status.ok()) {
    throw std::runtime_error("Could not create dir " + dir_path);
  }
  return std::make_shared<HiveTable>(identifier, dir_path, table_metadata, s3fs_);
}

}  // namespace iceberg::ice_tea
