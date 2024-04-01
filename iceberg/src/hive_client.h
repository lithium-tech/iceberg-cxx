#pragma once

#include <memory>
#include <string>

namespace iceberg {

class HiveClientImpl;

class HiveClient {
 public:
  HiveClient(const std::string& host, int port);

  ~HiveClient();

  std::string GetMetadataLocation(const std::string& db_name, const std::string& table_name) const;

 private:
  std::unique_ptr<HiveClientImpl> impl_;
};

}  // namespace iceberg
