#pragma once

#include <ThriftHiveMetastore.h>
#include <hive_metastore_types.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include <memory>
#include <string>

namespace ice_tea {

class MetastoreClient {
 public:
  MetastoreClient(const std::string& endpoint, int16_t port)
      : socket_(std::make_shared<apache::thrift::transport::TSocket>(endpoint, port)),
        transport_(std::make_shared<apache::thrift::transport::TBufferedTransport>(socket_)),
        protocol_(std::make_shared<apache::thrift::protocol::TBinaryProtocol>(transport_)),
        client_(protocol_) {
    transport_->open();
  }

  ~MetastoreClient() { transport_->close(); }

  Apache::Hadoop::Hive::ThriftHiveMetastoreClient& Get() { return client_; }

 private:
  std::shared_ptr<apache::thrift::transport::TTransport> socket_;
  std::shared_ptr<apache::thrift::transport::TTransport> transport_;
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol_;
  Apache::Hadoop::Hive::ThriftHiveMetastoreClient client_;
};

}  // namespace ice_tea
