#include <arrow/status.h>

#include <chrono>

#include <iceberg/catalog.h>
#include <iceberg/tea_scan.h>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "iceberg/common/fs/file_reader_provider_impl.h"
#include "iceberg/common/fs/filesystem_provider.h"
#include "iceberg/common/fs/filesystem_provider_impl.h"
#include "iceberg/equality_delete/handler.h"
#include "iceberg/result.h"
#include "iceberg/streams/iceberg/builder.h"
#include "iceberg/streams/iceberg/data_entries_meta_stream.h"
#include "iceberg/streams/iceberg/equality_delete_applier.h"
#include "iceberg/streams/iceberg/plan.h"
#include "iceberg/tea_hive_catalog.h"

struct S3FinalizerGuard {
  S3FinalizerGuard() {
    if (auto status = arrow::fs::InitializeS3(arrow::fs::S3GlobalOptions{}); !status.ok()) {
      throw status;
    }
  }

  ~S3FinalizerGuard() {
    try {
      if (arrow::fs::IsS3Initialized() && !arrow::fs::IsS3Finalized()) {
        arrow::fs::EnsureS3Finalized().ok();
      }
    } catch (...) {
    }
  }
};

class MetaStream : public iceberg::IAnnotatedDataPathStream {
 public:
  explicit MetaStream(iceberg::ice_tea::ScanMetadata&& planned_meta) {
    planned_meta_ = std::move(planned_meta);

    InitializeAllEntries();
  }

  std::shared_ptr<iceberg::AnnotatedDataPath> ReadNext() override {
    if (current_iterator_ == all_data_entries.size()) {
      return nullptr;
    }
    return std::make_shared<iceberg::AnnotatedDataPath>(all_data_entries[current_iterator_++]);
  }

 private:
  void InitializeAllEntries() {
    for (size_t partition_id = 0; partition_id < planned_meta_.partitions.size(); ++partition_id) {
      for (size_t layer_id_p1 = planned_meta_.partitions[partition_id].size(); layer_id_p1 >= 1; layer_id_p1--) {
        const auto layer_id = layer_id_p1 - 1;

        std::sort(planned_meta_.partitions[partition_id][layer_id].data_entries_.begin(),
                  planned_meta_.partitions[partition_id][layer_id].data_entries_.end(),
                  [&](const auto& lhs, const auto& rhs) { return lhs.path < rhs.path; });
        for (const auto& data_entry : planned_meta_.partitions[partition_id][layer_id].data_entries_) {
          std::vector<iceberg::AnnotatedDataPath::Segment> segments;
          for (const auto& part : data_entry.parts) {
            segments.emplace_back(iceberg::AnnotatedDataPath::Segment{.offset = part.offset, .length = part.length});
          }
          iceberg::PartitionLayerFile state(iceberg::PartitionLayer(partition_id, layer_id), data_entry.path);

          auto path = iceberg::AnnotatedDataPath(state, std::move(segments));
          all_data_entries.emplace_back(std::move(path));
        }
      }
    }
  }

  std::vector<iceberg::AnnotatedDataPath> all_data_entries;
  iceberg::ice_tea::ScanMetadata planned_meta_;
  size_t current_iterator_ = 0;
};

ABSL_FLAG(std::string, hms_host, "localhost", "host to connect to");
ABSL_FLAG(uint16_t, hms_port, 9083, "port to connect to");

ABSL_FLAG(std::string, db_name, "", "db_name");
ABSL_FLAG(std::string, table_name, "", "table_name");

ABSL_FLAG(std::string, s3_access_key_id, "", "s3_access_key_id");
ABSL_FLAG(std::string, s3_secret_access_key, "", "s3_secret_access_key");
ABSL_FLAG(std::string, s3_endpoint, "", "s3_endpoint");

int main(int argc, char** argv) {
  try {
    absl::ParseCommandLine(argc, argv);

    const std::string hms_host = absl::GetFlag(FLAGS_hms_host);
    const int hms_port = absl::GetFlag(FLAGS_hms_port);

    const std::string db_name = absl::GetFlag(FLAGS_db_name);
    const std::string table_name = absl::GetFlag(FLAGS_table_name);

    if (db_name == "") {
      std::cerr << "db_name is not set" << std::endl;
      return 1;
    }

    if (table_name == "") {
      std::cerr << "table_name is not set" << std::endl;
      return 1;
    }

    iceberg::ice_tea::HiveCatalog catalog(hms_host, hms_port);
    iceberg::catalog::TableIdentifier table_id{.db = db_name, .name = table_name};
    auto table = catalog.LoadTable(table_id);
    if (!table) {
      std::cerr << "No table" << std::endl;
      return 1;
    }

    const std::string metadata_location = table->Location();
    std::cout << "metadata_location = " << metadata_location << std::endl;

    const std::string s3_access_key_id = absl::GetFlag(FLAGS_s3_access_key_id);
    const std::string s3_secret_access_key = absl::GetFlag(FLAGS_s3_secret_access_key);
    const std::string s3_endpoint = absl::GetFlag(FLAGS_s3_endpoint);

    if (s3_access_key_id == "") {
      std::cerr << "s3_access_key_id is not set" << std::endl;
      return 1;
    }

    if (s3_secret_access_key == "") {
      std::cerr << "s3_secret_access_key is not set" << std::endl;
      return 1;
    }

    if (s3_endpoint == "") {
      std::cerr << "s3_endpoint is not set" << std::endl;
      return 1;
    }

    S3FinalizerGuard guard;
    auto s3options = arrow::fs::S3Options::FromAccessKey(s3_access_key_id, s3_secret_access_key);
    s3options.endpoint_override = absl::GetFlag(FLAGS_s3_endpoint);
    s3options.scheme = "http";

    auto maybe_fs = arrow::fs::S3FileSystem::Make(s3options);
    if (!maybe_fs.ok()) {
      std::cerr << maybe_fs.status() << std::endl;
      return 1;
    }
    auto fs = maybe_fs.MoveValueUnsafe();

    auto data = iceberg::ValueSafe(iceberg::ice_tea::ReadFile(fs, metadata_location));
    std::shared_ptr<iceberg::TableMetadataV2> table_metadata = iceberg::ice_tea::ReadTableMetadataV2(data);
    if (!table_metadata) {
      std::cerr << "GetScanMetadata: failed to parse metadata " + metadata_location << std::endl;
      return 1;
    }
    auto entries_stream = iceberg::ice_tea::AllEntriesStream::Make(fs, table_metadata, true);
    if (!entries_stream) {
      std::cerr << "Failed to make entries stream" << std::endl;
      return 1;
    }

    auto maybe_scan_metadata = iceberg::ice_tea::GetScanMetadata(*entries_stream, *table_metadata);
    if (!maybe_scan_metadata.ok()) {
      std::cerr << maybe_scan_metadata.status().ToString() << std::endl;
      return 1;
    }

    auto scan_metadata = maybe_scan_metadata.ValueUnsafe();
    auto meta_stream = std::make_shared<MetaStream>(std::move(scan_metadata));

    iceberg::S3FileSystemGetter::Config cfg;
    cfg.access_key = s3_access_key_id;
    cfg.secret_key = s3_secret_access_key;
    cfg.endpoint_override = s3_endpoint;
    cfg.scheme = "http";
    cfg.connect_timeout = std::chrono::milliseconds(1);
    cfg.request_timeout = std::chrono::milliseconds(3);
    cfg.retry_max_attempts = 3;

    std::shared_ptr<iceberg::IFileSystemGetter> s3_fs_getter = std::make_shared<iceberg::S3FileSystemGetter>(cfg);
    std::shared_ptr<iceberg::IFileSystemGetter> local_fs_getter = std::make_shared<iceberg::LocalFileSystemGetter>();

    std::map<std::string, std::shared_ptr<iceberg::IFileSystemGetter>> schema_to_getter{{"s3", s3_fs_getter},
                                                                                        {"file", local_fs_getter}};

    std::shared_ptr<iceberg::IFileSystemProvider> fs_provider =
        std::make_shared<iceberg::FileSystemProvider>(schema_to_getter);

    iceberg::PositionalDeletes pos_del_info;
    iceberg::EqualityDeletes eq_del_info;

    for (size_t partition_id = 0; partition_id < scan_metadata.partitions.size(); ++partition_id) {
      const auto& partition = scan_metadata.partitions.at(partition_id);
      for (size_t layer_id = 0; layer_id < partition.size(); ++layer_id) {
        const auto& layer = partition[layer_id];
        pos_del_info.delete_entries[partition_id][layer_id] = std::move(layer.positional_delete_entries_);
        eq_del_info.partlayer_to_deletes[partition_id][layer_id] = std::move(layer.equality_delete_entries_);
      }
    }

    // TODO(gmusya): make customizable
    iceberg::EqualityDeleteHandler::Config eq_del_config;
    eq_del_config.use_specialized_deletes = false;
    eq_del_config.equality_delete_max_mb_size = 10;
    eq_del_config.max_rows = 1'000'000;
    eq_del_config.throw_if_memory_limit_exceeded = false;

    auto schema_name_mapping = [table_metadata]() -> std::optional<std::string> {
      auto it = table_metadata->properties.find("schema.name-mapping.default");
      if (it == table_metadata->properties.end()) {
        return std::nullopt;
      }
      return it->second;
    }();

    auto data_stream = iceberg::IcebergScanBuilder::MakeIcebergStream(
        meta_stream, pos_del_info, std::make_shared<iceberg::EqualityDeletes>(std::move(eq_del_info)),
        std::move(eq_del_config), nullptr, nullptr, *table_metadata->GetCurrentSchema(), {1},
        std::make_shared<iceberg::FileReaderProvider>(fs_provider), std::move(schema_name_mapping));

    while (true) {
      auto batch = data_stream->ReadNext();
      if (!batch) {
        break;
      }
      std::cout << "path: " << batch->GetPath() << ", row_number = " << batch->GetRowPosition()
                << ", partition_id = " << batch->GetPartition() << ", layer_id = " << batch->GetLayer() << std::endl;
      std::cout << "batch: " << batch->GetRecordBatch()->ToString() << std::endl;
      std::cout << std::string(80, '-') << std::endl;
    }
  } catch (arrow::Status& s) {
    std::cerr << s.ToString() << std::endl;
    return 1;
  }
  return 0;
}
