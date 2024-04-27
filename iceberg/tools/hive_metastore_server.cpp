#include <ThriftHiveMetastore.h>
#include <hive_metastore_types.h>
#include <thrift/TToString.h>
#include <thrift/concurrency/ThreadFactory.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include <iostream>
#include <mutex>

class ThriftHiveMetastoreHandler : public Apache::Hadoop::Hive::ThriftHiveMetastoreNull {
  void get_all_databases(std::vector<std::string>& _return) override {
    std::lock_guard lg(mutex_);
    std::cerr << __FUNCTION__ << std::endl;
    _return.clear();
    for (const auto& [db_name, db_info] : tables_) {
      _return.emplace_back(db_name);
    }
  }

  void get_all_tables(std::vector<std::string>& _return, const std::string& db_name) override {
    std::lock_guard lg(mutex_);
    std::cerr << __FUNCTION__ << std::endl;
    _return.clear();
    auto db_it = tables_.find(db_name);
    if (db_it == tables_.end()) {
      return;
    }
    for (const auto& [table_name, table_info] : db_it->second) {
      _return.emplace_back(table_name);
    }
  }

  void get_table(Apache::Hadoop::Hive::Table& table, const std::string& db_name,
                 const std::string& table_name) override {
    std::lock_guard lg(mutex_);
    std::cerr << __FUNCTION__ << std::endl;
    auto db_it = tables_.find(db_name);
    if (db_it == tables_.end()) {
      return;
    }
    auto table_it = db_it->second.find(table_name);
    if (table_it == db_it->second.end()) {
      return;
    }
    table = table_it->second;
  }

  void create_table(const Apache::Hadoop::Hive::Table& table) override {
    std::lock_guard lg(mutex_);
    std::cerr << __FUNCTION__ << std::endl;
    tables_[table.dbName][table.tableName] = table;
  }

  std::mutex mutex_;
  std::map<std::string, std::map<std::string, Apache::Hadoop::Hive::Table>> tables_;
};

int main(int argc, char** argv) {
  apache::thrift::server::TThreadedServer server(
      std::make_shared<Apache::Hadoop::Hive::ThriftHiveMetastoreProcessor>(
          std::make_shared<ThriftHiveMetastoreHandler>()),
      std::make_shared<apache::thrift::transport::TServerSocket>(9090),
      std::make_shared<apache::thrift::transport::TBufferedTransportFactory>(),
      std::make_shared<apache::thrift::protocol::TBinaryProtocolFactory>());
  server.run();
  return 0;
}
