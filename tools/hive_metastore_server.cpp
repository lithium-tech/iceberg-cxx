#include "tools/hive_metastore_server.h"

#include <iostream>

int main(int argc, char** argv) {
  auto logger = [](const std::string& str) { std::cerr << str << std::endl; };

  apache::thrift::server::TThreadedServer server = iceberg::MakeHMSServer(9090, std::move(logger));
  server.run();
  return 0;
}
