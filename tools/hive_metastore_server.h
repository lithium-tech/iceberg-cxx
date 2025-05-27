#pragma once

#include <ThriftHiveMetastore.h>
#include <hive_metastore_types.h>

#include <map>
#include <memory>
#include <string>

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

#include <utility>
#include <vector>

namespace iceberg {

class ThriftHiveMetastoreHandler : public Apache::Hadoop::Hive::ThriftHiveMetastoreNull {
 public:
  using Logger = std::function<void(const std::string&)>;

  explicit ThriftHiveMetastoreHandler(Logger logger) : logger_(std::move(logger)) {}

  void get_all_databases(std::vector<std::string>& _return) override {
    std::lock_guard lg(mutex_);
    logger_(__FUNCTION__);
    _return.clear();
    for (const auto& [db_name, db_info] : tables_) {
      _return.emplace_back(db_name);
    }
  }

  void get_all_tables(std::vector<std::string>& _return, const std::string& db_name) override {
    std::lock_guard lg(mutex_);
    logger_(std::string(__FUNCTION__) + " " + db_name);
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
    logger_(std::string(__FUNCTION__) + " " + db_name + " " + table_name);
    auto db_it = tables_.find(db_name);
    if (db_it == tables_.end()) {
      Apache::Hadoop::Hive::NoSuchObjectException ex;
      ex.message = "no db '" + db_name + "'";
      throw ex;
    }
    auto table_it = db_it->second.find(table_name);
    if (table_it == db_it->second.end()) {
      Apache::Hadoop::Hive::NoSuchObjectException ex;
      ex.message = "no table '" + table_name + "'";
      throw ex;
    }
    table = table_it->second;
  }

  void create_database(const Apache::Hadoop::Hive::Database& database) override {
    std::lock_guard lg(mutex_);
    logger_(std::string(__FUNCTION__) + " " + database.name);
    auto db_it = tables_.find(database.name);
    if (db_it != tables_.end()) {
      Apache::Hadoop::Hive::InvalidObjectException ex;
      ex.message = "db '" + database.name + "' already exists";
      throw ex;
    }
    tables_[database.name] = {};
  }

  void drop_database(const std::string& db_name, const bool delete_data, const bool cascade) override {
    if (delete_data) {
      Apache::Hadoop::Hive::InvalidInputException ex;
      ex.message = "drop_table with delete_data set to true is not supported";
      throw ex;
    }
    if (cascade) {
      Apache::Hadoop::Hive::InvalidInputException ex;
      ex.message = "drop_table with cascade set to true is not supported";
      throw ex;
    }
    std::lock_guard lg(mutex_);
    logger_(std::string(__FUNCTION__) + " " + db_name);
    auto db_it = tables_.find(db_name);
    if (db_it == tables_.end()) {
      Apache::Hadoop::Hive::NoSuchObjectException ex;
      ex.message = "no db '" + db_name + "'";
      throw ex;
    }
    if (!db_it->second.empty()) {
      Apache::Hadoop::Hive::InvalidInputException ex;
      ex.message = "db '" + db_name + "' is not empty";
      throw ex;
    }
    tables_.erase(db_it);
  }

  void drop_table(const std::string& db_name, const std::string& table_name, const bool delete_data) override {
    if (delete_data) {
      Apache::Hadoop::Hive::InvalidInputException ex;
      ex.message = "drop_table with delete_data set to true is not supported";
      throw ex;
    }
    std::lock_guard lg(mutex_);
    logger_(std::string(__FUNCTION__) + " " + db_name + " " + table_name);
    auto db_it = tables_.find(db_name);
    if (db_it == tables_.end()) {
      Apache::Hadoop::Hive::NoSuchObjectException ex;
      ex.message = "no db '" + db_name + "'";
      throw ex;
    }
    auto table_it = db_it->second.find(table_name);
    if (table_it == db_it->second.end()) {
      Apache::Hadoop::Hive::NoSuchObjectException ex;
      ex.message = "no table '" + table_name + "'";
      throw ex;
    }
    tables_.at(db_name).erase(table_name);
  }

  void create_table(const Apache::Hadoop::Hive::Table& table) override {
    std::lock_guard lg(mutex_);
    logger_(std::string(__FUNCTION__) + " " + table.dbName + " " + table.tableName);
    tables_[table.dbName][table.tableName] = table;
  }

  std::mutex mutex_;
  std::map<std::string, std::map<std::string, Apache::Hadoop::Hive::Table>> tables_;
  Logger logger_;
};

static apache::thrift::server::TThreadedServer MakeHMSServer(uint16_t port, ThriftHiveMetastoreHandler::Logger logger) {
  return apache::thrift::server::TThreadedServer(
      std::make_shared<Apache::Hadoop::Hive::ThriftHiveMetastoreProcessor>(
          std::make_shared<iceberg::ThriftHiveMetastoreHandler>(std::move(logger))),
      std::make_shared<apache::thrift::transport::TServerSocket>(port),
      std::make_shared<apache::thrift::transport::TBufferedTransportFactory>(),
      std::make_shared<apache::thrift::protocol::TBinaryProtocolFactory>());
}

}  // namespace iceberg
