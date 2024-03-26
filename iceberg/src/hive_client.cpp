#include "iceberg/src/hive_client.h"

#include <ThriftHiveMetastore.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include <map>
#include <stdexcept>

namespace iceberg {

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

  std::string GetMetadataLocation(const std::string& db_name,
                                  const std::string& table_name) {
    Apache::Hadoop::Hive::Table table;
    client_.get_table(table, db_name, table_name);
    const std::map<std::string, std::string>& params = table.parameters;
    if (!params.contains("metadata_location")) {
      throw std::runtime_error(
          "Table metadata does not contain metadata_location");
    }
    return params.at("metadata_location");
  }

 private:
  std::shared_ptr<apache::thrift::transport::TTransport> socket_;
  std::shared_ptr<apache::thrift::transport::TTransport> transport_;
  std::shared_ptr<apache::thrift::protocol::TBinaryProtocol> protocol_;
  Apache::Hadoop::Hive::ThriftHiveMetastoreClient client_;
};

HiveClient::HiveClient(const std::string& host, int port)
    : impl_(std::make_unique<HiveClientImpl>(host, port)) {}

HiveClient::~HiveClient() = default;

std::string HiveClient::GetMetadataLocation(
    const std::string& db_name, const std::string& table_name) const {
  return impl_->GetMetadataLocation(db_name, table_name);
}

}  // namespace iceberg
