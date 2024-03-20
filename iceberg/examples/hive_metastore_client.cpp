#include <ThriftHiveMetastore.h>
#include <hive_metastore_types.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include <iostream>
#include <stdexcept>

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace Apache::Hadoop::Hive;

namespace {

enum class Mode {
  kGetTables,
  kGetTable,
  kGetDatabases,
  kCreateTable,
  kUnknown,
};

struct ModeStringEntry {
  std::string_view name;
  Mode mode;
};

constexpr ModeStringEntry kModeStringEntries[] = {
    {"get-tables", Mode::kGetTables},
    {"get-table", Mode::kGetTable},
    {"get-databases", Mode::kGetDatabases},
    {"create-table", Mode::kCreateTable}};

Mode StringToMode(const std::string& str) {
  for (const auto& [name, mode] : kModeStringEntries) {
    if (name == str) {
      return mode;
    }
  }
  return Mode::kUnknown;
}

void PrintSupportModes(std::ostream& os) {
  os << "Supported modes: ";
  for (const auto& [name, mode] : kModeStringEntries) {
    os << name << " ";
  }
  os << std::endl;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc < 4) {
    std::cerr << "Usage: " << argv[0] << " <mode> <endpoint> <port> ..."
              << std::endl;
    PrintSupportModes(std::cerr);
    return 1;
  }
  const Mode mode = StringToMode(argv[1]);
  const std::string endpoint = argv[2];
  const int port = std::stoi(argv[3]);
  if (mode == Mode::kUnknown) {
    PrintSupportModes(std::cerr);
    return 1;
  }
  std::shared_ptr<TTransport> socket(new TSocket(endpoint, port));
  std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
  ThriftHiveMetastoreClient client(protocol);

  try {
    transport->open();
    switch (mode) {
      case Mode::kGetTables: {
        if (argc != 5) {
          std::cerr << "Usage: " << argv[0]
                    << " get-tables <endpoint> <port> <db_name>" << std::endl;
          return 1;
        }
        const std::string db_name = argv[4];
        std::vector<std::string> tables;
        client.get_all_tables(tables, db_name);
        for (const auto& table_name : tables) {
          std::cout << table_name << std::endl;
        }
        break;
      }
      case Mode::kGetTable: {
        if (argc != 6) {
          std::cerr << "Usage: " << argv[0]
                    << " get-table <endpoint> <port> <db_name> <table_name>"
                    << std::endl;
          return 1;
        }
        const std::string db_name = argv[4];
        const std::string table_name = argv[5];
        Table table;
        client.get_table(table, db_name, table_name);
        table.printTo(std::cout);
        break;
      }
      case Mode::kGetDatabases: {
        if (argc != 4) {
          std::cerr << "Usage: " << argv[0] << " get-table <endpoint> <port>"
                    << std::endl;
          return 1;
        }
        std::vector<std::string> databases;
        client.get_all_databases(databases);
        for (const auto& name : databases) {
          std::cout << name << std::endl;
        }
        break;
      }
      case Mode::kCreateTable: {
        if (argc != 7) {
          std::cerr << "Usage: " << argv[0]
                    << " create-table <endpoint> <port> <db_name> <table_name> "
                       "<metadata_location>"
                    << std::endl;
          return 1;
        }
        const std::string db_name = argv[4];
        const std::string table_name = argv[5];
        const std::string metadata_location = argv[6];
        Table table;
        table.dbName = db_name;
        table.tableName = table_name;
        table.parameters["metadata_location"] = metadata_location;
        client.create_table(table);
        break;
      }
      default:
        throw std::runtime_error("Unknown mode");
    }
    transport->close();
  } catch (TException& tx) {
    cout << "ERROR: " << tx.what() << endl;
  }
}
