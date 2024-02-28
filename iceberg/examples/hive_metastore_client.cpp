#include <ThriftHiveMetastore.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include <iostream>

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace Apache::Hadoop::Hive;

int main(int argc, char** argv) {
  if (argc < 4) {
    std::cerr << "Supportet modes: get-tables, get-table" << std::endl;
    std::cerr << "Usage: " << argv[0] << " <mode> <endpoint> <port> ..."
              << std::endl;
    return 1;
  }
  const std::string mode = argv[1];
  const std::string endpoint = argv[2];
  const int port = std::stoi(argv[3]);
  if (mode != "get-tables" && mode != "get-table") {
    std::cerr << "Supportet modes: get-tables, get-table" << std::endl;
    return 1;
  }
  if (mode == "get-tables" && argc != 5) {
    std::cerr << "Usage: " << argv[0]
              << " get-tables <endpoint> <port> <db_name>" << std::endl;
    return 1;
  }
  if (mode == "get-table" && argc != 6) {
    std::cerr << "Usage: " << argv[0]
              << " get-table <endpoint> <port> <db_name> <table_name>"
              << std::endl;
    return 1;
  }
  const std::string db_name = argv[4];
  std::shared_ptr<TTransport> socket(new TSocket(endpoint, port));
  std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  ThriftHiveMetastoreClient client(protocol);

  try {
    transport->open();
    if (mode == "get-tables") {
      std::vector<std::string> tables;
      client.get_tables(tables, db_name, "*");

      for (const auto& table_name : tables) {
        std::cout << table_name << std::endl;
      }
    } else /* mode == get-table */ {
      const std::string table_name = argv[5];
      Table table;
      client.get_table(table, db_name, table_name);
      table.printTo(std::cout);
    }

    transport->close();
  } catch (TException& tx) {
    cout << "ERROR: " << tx.what() << endl;
  }
}
