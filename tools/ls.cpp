#include <ThriftHiveMetastore.h>
#include <absl/flags/flag.h>
#include <absl/flags/parse.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>

#include <hive_metastore_types.h>

#include <iostream>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include "iceberg/common/error.h"
#include "iceberg/nested_field.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
#include "tools/metadata_tree.h"
#include "tools/metastore_client.h"
#include "tools/s3client.h"

namespace hive = Apache::Hadoop::Hive;
namespace thrift = apache::thrift;

using iceberg::tools::MetadataTree;
using iceberg::tools::S3Client;
using iceberg::tools::S3Init;
using iceberg::tools::StringFix;

namespace {

bool CopyDir(std::shared_ptr<S3Client> s3client, const std::string& src, const std::string& dst, bool use_threads) {
  iceberg::Ensure(s3client != nullptr, std::string("cannot copy dir ") + src + " to " + dst);
  return s3client->CopyDir(src, dst, use_threads);
}

std::string GpTypeStr(const iceberg::types::Type* type) {
  using iceberg::TypeID;

  Ensure(type != nullptr, "cannot convert type - nullptr");

  switch (type->TypeId()) {
    case TypeID::kBoolean:
      return "BOOLEAN";
    case TypeID::kInt:
      return "INTEGER";
    case TypeID::kLong:
      return "BIGINT";
    case TypeID::kFloat:
      return "FLOAT";
    case TypeID::kDouble:
      return "DOUBLE PRECISION";
    case TypeID::kDecimal:
      if (auto* decimal = dynamic_cast<const iceberg::types::DecimalType*>(type)) {
        return "NUMERIC(" + std::to_string(decimal->Precision()) + "," + std::to_string(decimal->Scale()) + ")";
      }
      break;
    case TypeID::kDate:
      return "DATE";
    case TypeID::kTime:
      return "TIME WITHOUT TIME ZONE";
    case TypeID::kTimestamp:
      return "TIMESTAMP WITHOUT TIME ZONE";
    case TypeID::kTimestamptz:
      return "TIMESTAMP WITH TIME ZONE";
    case TypeID::kString:
      return "TEXT";
    case TypeID::kUuid:
      return "UUID";
    case TypeID::kFixed:
      break;  // not supported
    case TypeID::kBinary:
      return "BYTEA";
    case TypeID::kStruct:
      break;  // not supported
    case TypeID::kList:
      if (auto* list = dynamic_cast<const iceberg::types::ListType*>(type)) {
        return GpTypeStr(list->ElementType().get()) + "[]";
      }
      break;
    case TypeID::kMap:
      break;  // not supported
    case TypeID::kUnknown:
      break;
  }
  throw std::runtime_error("cannot convert type: " + type->ToString());
}

std::string FieldStr(const iceberg::types::NestedField& field, bool gp_type) {
  std::string col_descr = field.name;
  if (field.type) {
    col_descr += " " + (gp_type ? GpTypeStr(field.type.get()) : field.type->ToString());
  }
  if (field.is_required) {
    col_descr += " NOT NULL";
  }
  return col_descr;
}

}  // namespace

static constexpr uint16_t HMS_PORT = 9083;

ABSL_FLAG(std::string, host, "localhost", "src HMS host");
ABSL_FLAG(uint16_t, port, HMS_PORT, "src HMS port");
ABSL_FLAG(std::string, db, "", "src database name");
ABSL_FLAG(std::string, table, "", "src table name");
ABSL_FLAG(std::string, tmpdir, "/tmp/ice_ls", "path to tmp directory");
ABSL_FLAG(bool, print_location, true, "print metadata location");
ABSL_FLAG(bool, print_files, false, "print file paths and types");
ABSL_FLAG(bool, print_schema, true, "print schema");
ABSL_FLAG(std::string, convert_types, "", "convert schema types, one of: GP, <empty>");
ABSL_FLAG(std::string, loglevel, "", "S3 SDK loglevel, one of: off, fatal, error, warn, info, debug, trace");

int main(int argc, char** argv) {
  try {
    absl::ParseCommandLine(argc, argv);

    const std::string src_host = absl::GetFlag(FLAGS_host);
    const uint16_t src_port = absl::GetFlag(FLAGS_port);
    const std::string src_db = absl::GetFlag(FLAGS_db);
    const std::string src_tablename = absl::GetFlag(FLAGS_table);
    const std::filesystem::path tmpdir = absl::GetFlag(FLAGS_tmpdir);
    const bool print_location = absl::GetFlag(FLAGS_print_location);
    const bool print_files = absl::GetFlag(FLAGS_print_files);
    const bool print_schema = absl::GetFlag(FLAGS_print_schema);
    const std::string convert_types = absl::GetFlag(FLAGS_convert_types);
    const std::string loglevel = absl::GetFlag(FLAGS_loglevel);

    bool gp_types = (convert_types == "GP");

    iceberg::Ensure(!tmpdir.empty(), "Wrong args: tmpdir should not be empty");

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

    iceberg::Ensure(src_table.parameters.contains("metadata_location"),
                    std::string("no 'metadata_location' parameter for table '") + src_tablename + "'");

    const std::filesystem::path src_meta_json = src_table.parameters["metadata_location"];
    std::string src_metadata_path = std::filesystem::path(src_meta_json).parent_path();

    const std::filesystem::path meta_tmpdir = tmpdir / src_tablename / "metadata";
    std::filesystem::remove_all(meta_tmpdir);
    std::filesystem::create_directories(meta_tmpdir);

    auto s3client = std::make_shared<S3Client>(false, S3Init::LogLevel(loglevel), S3Client::CHUNK_SIZE,
                                               std::string("AWS_"), std::string());

    std::cerr << "copying table '" << src_tablename << "' meta form " << src_metadata_path << " to " << meta_tmpdir
              << std::endl;
    iceberg::Ensure(CopyDir(s3client, src_metadata_path, meta_tmpdir, false),
                    std::string("cannot copy dir ") + src_metadata_path + " to " + meta_tmpdir.string());

    std::filesystem::path meta_tmpdir_json = meta_tmpdir / src_meta_json.filename();
    std::shared_ptr<iceberg::TableMetadataV2> table_metadata;
    {
      iceberg::Ensure(std::filesystem::is_regular_file(meta_tmpdir_json),
                      "No metadata file '" + meta_tmpdir_json.string() + "'");

      std::ifstream input_metadata(meta_tmpdir_json.string());
      table_metadata = iceberg::ice_tea::ReadTableMetadataV2(input_metadata);
      iceberg::Ensure(table_metadata != nullptr, "Cannot read metadata file '" + meta_tmpdir_json.string() + "'");
    }

    if (print_location) {
      std::cout << "location: " << table_metadata->location << std::endl;
      std::cout << "json: " << src_meta_json << std::endl;
    }

    if (print_files) {
      std::vector<MetadataTree> prev_meta;
      MetadataTree meta_tree(meta_tmpdir_json);
      LoadTree(meta_tree, meta_tmpdir_json, prev_meta, false);

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
      std::cout << "-- " << (gp_types ? "GP" : "Iceberg") << " schema for " << src_db << "." << src_tablename
                << std::endl;
      std::cout << "CREATE TABLE " << src_db << "." << src_tablename << std::endl << "(" << std::endl;
      std::shared_ptr<iceberg::Schema> schema = table_metadata->schemas[current_schema_id];
      const auto& columns = schema->Columns();
      for (size_t i = 0; i < columns.size(); ++i) {
        const iceberg::types::NestedField& field = columns[i];
        bool has_next = columns.size() - i - 1;
        std::cout << "  " << FieldStr(field, gp_types) << (has_next ? "," : "") << std::endl;
      }
      std::cout << ")" << std::endl;
    }
  } catch (std::exception& ex) {
    std::cerr << ex.what() << std::endl;
    return 1;
  }

  return 0;
}
