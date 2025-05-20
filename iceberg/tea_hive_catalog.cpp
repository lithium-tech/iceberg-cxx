#include "iceberg/tea_hive_catalog.h"

#include <ThriftHiveMetastore.h>

#include <memory>
#include <stdexcept>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include <vector>

#include "arrow/api.h"
#include "iceberg/tea_remote_catalog.h"
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

  bool TableExists(const std::string& db_name, const std::string& table_name) {
    return !GetTable(db_name, table_name).tableName.empty();
  }

 private:
  std::shared_ptr<apache::thrift::transport::TTransport> socket_;
  std::shared_ptr<apache::thrift::transport::TTransport> transport_;
  std::shared_ptr<apache::thrift::protocol::TBinaryProtocol> protocol_;
  Apache::Hadoop::Hive::ThriftHiveMetastoreClient client_;
};

HiveClient::HiveClient(const std::string& host, int port) : client_(std::make_shared<HiveClientImpl>(host, port)) {}

std::string HiveClient::GetMetadataLocation(const std::string& db_name, const std::string& table_name) {
  return client_->GetMetadataLocation(db_name, table_name);
}

bool HiveClient::TableExists(const std::string& db_name, const std::string& table_name) {
  return client_->TableExists(db_name, table_name);
}

HiveCatalog::HiveCatalog(const std::string& host, int port, std::shared_ptr<arrow::fs::S3FileSystem> s3fs)
    : RemoteCatalog(std::make_shared<HiveClient>(host, port), s3fs) {}

}  // namespace iceberg::ice_tea
