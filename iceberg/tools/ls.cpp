#include <ThriftHiveMetastore.h>
#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include <hive_metastore_types.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>

#include "iceberg/src/nested_field.h"
#include "iceberg/src/schema.h"
#include "iceberg/src/table_metadata.h"
#include "iceberg/tools/metadata_tree.h"
#include "iceberg/tools/metastore_client.h"
#include "iceberg/tools/s3client.h"

namespace hive = Apache::Hadoop::Hive;
namespace thrift = apache::thrift;

using iceberg::tools::MetadataTree;
using iceberg::tools::StringFix;

static constexpr uint16_t HMS_PORT = 9083;

ABSL_FLAG(std::string, host, "localhost", "src HMS host");
ABSL_FLAG(uint16_t, port, HMS_PORT, "src HMS port");
ABSL_FLAG(std::string, db, "", "src database name");
ABSL_FLAG(std::string, table, "", "src table name");
ABSL_FLAG(std::string, tmpdir, "/tmp/ice_ls", "path to tmp directory");
ABSL_FLAG(bool, rclone, false, "use rclone for sync");
ABSL_FLAG(bool, print_files, true, "print file paths and types");
ABSL_FLAG(bool, print_schema, true, "print schema");
ABSL_FLAG(std::string, loglevel, "", "S3 SDK loglevel, one of: off, fatal, error, warn, info, debug, trace");

int main(int argc, char** argv) {
  try {
    absl::ParseCommandLine(argc, argv);

    const std::string src_host = absl::GetFlag(FLAGS_host);
    const uint16_t src_port = absl::GetFlag(FLAGS_port);
    const std::string src_db = absl::GetFlag(FLAGS_db);
    const std::string src_tablename = absl::GetFlag(FLAGS_table);
    const std::filesystem::path tmpdir = absl::GetFlag(FLAGS_tmpdir);
    const bool use_rclone = absl::GetFlag(FLAGS_rclone);
    const bool print_files = absl::GetFlag(FLAGS_print_files);
    const bool print_schema = absl::GetFlag(FLAGS_print_schema);
    const std::string loglevel = absl::GetFlag(FLAGS_loglevel);

    if (tmpdir.empty()) {
      throw std::runtime_error("Wrong args: tmpdir should not be empty");
    }

    hive::Table src_table;
    {
      ice_tea::MetastoreClient src_client(src_host, src_port);

      if (src_db.empty()) {
        std::vector<std::string> databases;
        src_client.Get().get_all_databases(databases);

        std::cout << "db is not set. Known databases:" << std::endl;
        for (auto& db : databases) {
          std::cout << db << std::endl;
        }
        return 0;
      }
      if (src_tablename.empty()) {
        std::vector<std::string> tables;
        src_client.Get().get_all_tables(tables, src_db);

        std::cout << "table is not set. Known tables:" << std::endl;
        for (auto& table : tables) {
          std::cout << table << std::endl;
        }
        return 0;
      } else {
        src_client.Get().get_table(src_table, src_db, src_tablename);
      }
    }

    if (!src_table.parameters.contains("metadata_location")) {
      throw std::runtime_error(std::string("no 'metadata_location' parameter for table '") + src_tablename + "'");
    }
    const std::filesystem::path src_meta_json = src_table.parameters["metadata_location"];
    std::string src_metadata_path = std::filesystem::path(src_meta_json).parent_path();

    const std::filesystem::path meta_tmpdir = tmpdir / src_tablename / "metadata";
    std::filesystem::remove_all(meta_tmpdir);
    std::filesystem::create_directories(meta_tmpdir);

    std::shared_ptr<ice_tea::S3Client> s3client;
    if (!use_rclone) {
      s3client = std::make_shared<ice_tea::S3Client>(false, ice_tea::S3Init::LogLevel(loglevel), std::string("AWS_"),
                                                     std::string());
    }

    std::cerr << "copying table '" << src_tablename << "' meta form " << src_metadata_path << " to " << meta_tmpdir
              << std::endl;
    if (!ice_tea::CopyDir(s3client, src_metadata_path, meta_tmpdir, false)) {
      throw std::runtime_error(std::string("cannot copy dir ") + src_metadata_path + " to " + meta_tmpdir.string());
    }

    std::filesystem::path meta_tmpdir_json = meta_tmpdir / src_meta_json.filename();
    std::shared_ptr<iceberg::TableMetadataV2> table_metadata;
    {
      if (!std::filesystem::is_regular_file(meta_tmpdir_json)) {
        throw std::runtime_error("No metadata file '" + meta_tmpdir_json.string() + "'");
      }
      std::ifstream input_metadata(meta_tmpdir_json.string());
      table_metadata = iceberg::ice_tea::ReadTableMetadataV2(input_metadata);
      if (!table_metadata) {
        throw std::runtime_error("Cannot read metadata file '" + meta_tmpdir_json.string() + "'");
      }
    }
    std::cerr << "location: " << table_metadata->location << std::endl;

    if (print_files) {
      std::vector<MetadataTree> prev_meta;
      std::unordered_map<std::string, std::string> renames;
      MetadataTree meta_tree = FixLocation(meta_tmpdir_json, {}, {}, prev_meta, renames);

      for (auto& prev_tree : prev_meta) {
        prev_tree.Print(std::cout, 0);
        std::cout << std::endl;
      }
      meta_tree.Print(std::cout, 0);
      std::cout << std::endl;
    }

    if (print_schema) {
      int32_t current_schema_id = table_metadata->current_schema_id;
      if ((size_t)current_schema_id >= table_metadata->schemas.size() || !table_metadata->schemas[current_schema_id]) {
        std::cerr << "no schema for current_schema_id " << current_schema_id << std::endl;
      }
      std::cout << "-- Iceberg schema for " << src_db << "." << src_tablename << std::endl;
      std::cout << "CREATE TABLE " << src_db << "." << src_tablename << std::endl << "(" << std::endl;
      std::shared_ptr<iceberg::Schema> schema = table_metadata->schemas[current_schema_id];
      const auto& columns = schema->Columns();
      for (size_t i = 0; i < columns.size(); ++i) {
        const iceberg::types::NestedField& field = columns[i];
        bool has_next = columns.size() - i - 1;
        std::cout << "  " << field.name << " " << (field.type ? field.type->ToString() : std::string())
                  << (field.is_required ? " NOT NULL" : "") << (has_next ? "," : "") << std::endl;
      }
      std::cout << ");" << std::endl;
    }
  } catch (std::exception& ex) {
    std::cerr << ex.what() << std::endl;
    return 1;
  }

  return 0;
}
