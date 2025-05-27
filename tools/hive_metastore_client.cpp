#include "tools/hive_metastore_client.h"

#include <ThriftHiveMetastore.h>
#include <hive_metastore_types.h>

#include <iostream>
#include <stdexcept>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include <vector>

namespace {

enum class Mode {
  kGetTables,
  kGetTable,
  kCreateTable,
  kGetDatabases,
  kGetDatabase,
  kCreateDatabase,
  kGetPartitions,
  kGetSchema,
  kUpdateTableLocation,
  kUnknown,
};

struct ModeStringEntry {
  std::string_view name;
  Mode mode;
};

constexpr ModeStringEntry kModeStringEntries[] = {
    {"get-tables", Mode::kGetTables},
    {"get-table", Mode::kGetTable},
    {"create-table", Mode::kCreateTable},
    {"get-databases", Mode::kGetDatabases},
    {"get-database", Mode::kGetDatabase},
    {"create-database", Mode::kCreateDatabase},
    {"get-partitions", Mode::kGetPartitions},
    {"get-schema", Mode::kGetSchema},
    {"update-table-location", Mode::kUpdateTableLocation},
};

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
  using Apache::Hadoop::Hive::Database;
  using Apache::Hadoop::Hive::FieldSchema;
  using Apache::Hadoop::Hive::Partition;
  using Apache::Hadoop::Hive::Table;
  using Apache::Hadoop::Hive::ThriftHiveMetastoreClient;
  using apache::thrift::protocol::TBinaryProtocol;
  using apache::thrift::protocol::TProtocol;
  using apache::thrift::transport::TBufferedTransport;
  using apache::thrift::transport::TSocket;
  using apache::thrift::transport::TTransport;

  if (argc < 4) {
    std::cerr << "Usage: " << argv[0] << " <mode> <endpoint> <port> ..." << std::endl;
    PrintSupportModes(std::cerr);
    return 1;
  }
  const std::string prog_name = argv[0];
  const std::string str_mode = argv[1];
  const Mode mode = StringToMode(str_mode);
  const std::string endpoint = argv[2];
  const int port = std::stoi(argv[3]);
  if (mode == Mode::kUnknown) {
    PrintSupportModes(std::cerr);
    return 1;
  }

  iceberg::HiveMetastoreClient hms_client(endpoint, port);

  try {
    switch (mode) {
      case Mode::kGetTables: {
        if (argc != 5) {
          std::cerr << "Usage: " << prog_name << " " << str_mode << " <endpoint> <port> <db_name>" << std::endl;
          return 1;
        }
        const std::string db_name = argv[4];
        std::vector<std::string> tables = hms_client.GetTables(db_name);
        for (const auto& table_name : tables) {
          std::cout << table_name << std::endl;
        }
        break;
      }
      case Mode::kGetTable: {
        if (argc != 6) {
          std::cerr << "Usage: " << prog_name << " " << str_mode << " <endpoint> <port> <db_name> <table_name>"
                    << std::endl;
          return 1;
        }
        const std::string db_name = argv[4];
        const std::string table_name = argv[5];
        Table table = hms_client.GetTable(db_name, table_name);
        table.printTo(std::cout);
        break;
      }
      case Mode::kGetDatabases: {
        if (argc != 4) {
          std::cerr << "Usage: " << prog_name << " " << str_mode << " <endpoint> <port>" << std::endl;
          return 1;
        }
        std::vector<std::string> databases = hms_client.GetDatabases();
        for (const auto& name : databases) {
          std::cout << name << std::endl;
        }
        break;
      }
      case Mode::kGetDatabase: {
        if (argc != 5) {
          std::cerr << "Usage: " << prog_name << " " << str_mode << " <endpoint> <port> <db_name>" << std::endl;
          return 1;
        }
        const std::string db_name = argv[4];
        Database db = hms_client.GetDatabase(db_name);
        db.printTo(std::cout);
        break;
      }
      case Mode::kCreateDatabase: {
        if (argc != 6) {
          std::cerr << "Usage: " << prog_name << " " << str_mode << " <endpoint> <port> <db_name> <location>"
                    << std::endl;
          return 1;
        }
        const std::string db_name = argv[4];
        const std::string location = argv[5];
        hms_client.CreateDatabase(db_name, location);
        break;
      }
      case Mode::kCreateTable: {
        if (argc != 7) {
          std::cerr << "Usage: " << prog_name << " " << str_mode
                    << " <endpoint> <port> <db_name> <table_name> <metadata_location>" << std::endl;
          return 1;
        }
        const std::string db_name = argv[4];
        const std::string table_name = argv[5];
        const std::string metadata_location = argv[6];
        hms_client.CreateTable(db_name, table_name, metadata_location);
        break;
      }
      case Mode::kUpdateTableLocation: {
        if (argc != 7) {
          std::cerr << "Usage: " << prog_name << " " << str_mode
                    << " <endpoint> <port> <db_name> <table_name> <metadata_location>" << std::endl;
          return 1;
        }
        const std::string db_name = argv[4];
        const std::string table_name = argv[5];
        const std::string metadata_location = argv[6];
        hms_client.UpdateTableLocation(db_name, table_name, metadata_location);
        break;
      }
      case Mode::kGetPartitions: {
        if (argc != 6) {
          std::cerr << "Usage: " << prog_name << " " << str_mode << " <endpoint> <port> <db_name> <table_name>"
                    << std::endl;
          return 1;
        }
        const std::string db_name = argv[4];
        const std::string table_name = argv[5];
        const int16_t max_parts = 1024;
        std::vector<Partition> parts = hms_client.GetPartitions(db_name, table_name, max_parts);
        for (const auto& part : parts) {
          part.printTo(std::cout);
        }
        break;
      }
      case Mode::kGetSchema: {
        if (argc != 6) {
          std::cerr << "Usage: " << prog_name << " " << str_mode << " <endpoint> <port> <db_name> <table_name>"
                    << std::endl;
          return 1;
        }
        const std::string db_name = argv[4];
        const std::string table_name = argv[5];
        std::vector<FieldSchema> fields = hms_client.GetSchema(db_name, table_name);
        for (const auto& field : fields) {
          field.printTo(std::cout);
        }
        break;
      }
      default:
        throw std::runtime_error("Unknown mode");
    }
  } catch (const std::exception& ex) {
    std::cout << "ERROR: " << ex.what() << std::endl;
  }
}
