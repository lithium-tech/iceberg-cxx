#pragma once

#include <ThriftHiveMetastore.h>

#include <filesystem>

#include <hive_metastore_types.h>

#include <memory>
#include <string>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include <vector>

namespace iceberg {

using TBufferedTransport = apache::thrift::transport::TBufferedTransport;
using TSocket = apache::thrift::transport::TSocket;
using TTransport = apache::thrift::transport::TTransport;

static std::shared_ptr<TTransport> MakeTTransport(const std::string& endpoint, uint16_t port) {
  std::shared_ptr<TTransport> socket(new TSocket(endpoint, port));
  std::shared_ptr<TTransport> transport(new TBufferedTransport(socket), [](TTransport* transport) {
    if (transport) {
      transport->close();
    }
  });
  transport->open();
  return transport;
}

class HiveMetastoreClient {
  using Database = Apache::Hadoop::Hive::Database;
  using FieldSchema = Apache::Hadoop::Hive::FieldSchema;
  using Partition = Apache::Hadoop::Hive::Partition;
  using Table = Apache::Hadoop::Hive::Table;
  using ThriftHiveMetastoreClient = Apache::Hadoop::Hive::ThriftHiveMetastoreClient;

  using TBinaryProtocol = apache::thrift::protocol::TBinaryProtocol;
  using TProtocol = apache::thrift::protocol::TProtocol;

 public:
  HiveMetastoreClient(const std::string& endpoint, uint16_t port) {
    transport_ = MakeTTransport(endpoint, port);
    std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport_));
    client_.reset(new ThriftHiveMetastoreClient(protocol));
  }

  std::vector<std::string> GetTables(const std::string& db_name) {
    std::vector<std::string> result;
    client_->get_all_tables(result, db_name);
    return result;
  }

  Table GetTable(const std::string& db_name, const std::string& table_name) {
    Table table;
    client_->get_table(table, db_name, table_name);
    return table;
  }

  std::vector<std::string> GetDatabases() {
    std::vector<std::string> databases;
    client_->get_all_databases(databases);
    return databases;
  }

  Database GetDatabase(const std::string& db_name) {
    Database database;
    client_->get_database(database, db_name);
    return database;
  }

  void CreateDatabase(const std::string& db_name, const std::string& location) {
    Database db;
    db.name = db_name;
    db.locationUri = location;
    db.ownerName = "root";
    client_->create_database(db);
  }

  void DropDatabase(const std::string& db_name) { client_->drop_database(db_name, false, false); }

  void CreateTable(const std::string& db_name, const std::string& table_name, const std::string& metadata_location) {
    Table table;
    table.dbName = db_name;
    table.tableName = table_name;
    table.owner = "root";
    table.tableType = "EXTERNAL_TABLE";
    table.temporary = false;
    table.rewriteEnabled = false;
    table.sd.location = std::filesystem::path(metadata_location).parent_path().parent_path();
    table.parameters["metadata_location"] = metadata_location;
    table.parameters["EXTERNAL"] = "TRUE";
    // table.parameters["snapshot-count"] = "1";
    table.parameters["table_type"] = "ICEBERG";  // required field for HMS
    table.parameters["write.format.default"] = "PARQUET";
    // table.parameters["write.merge.mode"] = "copy-on-write";
    client_->create_table(table);
  }

  void UpdateTableLocation(const std::string& db_name, const std::string& table_name,
                           const std::string& metadata_location) {
    Table table = GetTable(db_name, table_name);
    table.parameters["metadata_location"] = metadata_location;
    client_->alter_table(db_name, table_name, table);
  }

  std::vector<Partition> GetPartitions(const std::string& db_name, const std::string& table_name,
                                       const int16_t max_parts) {
    std::vector<Partition> parts;
    client_->get_partitions(parts, db_name, table_name, max_parts);
    return parts;
  }

  std::vector<FieldSchema> GetSchema(const std::string& db_name, const std::string& table_name) {
    std::vector<FieldSchema> fields;
    client_->get_schema(fields, db_name, table_name);
    return fields;
  }

  void DropTable(const std::string& db_name, const std::string& table_name) {
    client_->drop_table(db_name, table_name, false);
  }

 private:
  std::shared_ptr<TTransport> transport_;
  std::unique_ptr<ThriftHiveMetastoreClient> client_;
};

}  // namespace iceberg
