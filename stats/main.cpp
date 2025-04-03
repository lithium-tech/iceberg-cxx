#include <exception>
#include <iostream>
#include <sstream>
#include <string>
#include <variant>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "arrow/filesystem/filesystem.h"
#include "arrow/filesystem/localfs.h"
#include "arrow/filesystem/s3fs.h"
#include "arrow/status.h"
#include "iceberg/puffin.h"
#include "iceberg/table_metadata.h"
#include "iceberg/tea_scan.h"
#include "iceberg/write.h"
#include "parquet/types.h"
#include "stats/analyzer.h"
#include "stats/datasketch/distinct_theta.h"
#include "stats/parquet/distinct.h"
#include "stats/parquet/frequent_item.h"
#include "stats/parquet/quantiles.h"
#include "theta_sketch.hpp"

template <typename T1, typename T2>
std::ostream& operator<<(std::ostream& os, const std::pair<T1, T2>& p) {
  return os << "(" << p.first << ", " << p.second << ")";
}

template <typename T>
std::ostream& operator<<(std::ostream& os, const std::vector<T>& p) {
  os << "[";
  bool is_first = true;
  for (const auto& elem : p) {
    if (is_first) {
      is_first = false;
    } else {
      os << ", ";
    }
    os << elem;
  }
  os << "]";
  return os;
}

namespace stats {

void PrintAnalyzeColumnResult(const std::string& name, const AnalyzeColumnResult& result, bool verbose = false) {
  if (result.counter.has_value()) {
    std::cerr << name << " has " << result.counter->GetNumberOfDistinctValues() << " distinct values" << std::endl;

    if (verbose) {
      const auto& impl = result.counter->GetState();
      if (auto ptr = std::get_if<stats::ThetaDistinctCounterWrapper>(&impl)) {
        ptr->Evaluate([&name](const stats::ThetaDistinctCounter& sketch) {
          std::cout << "Verbose state for column '" << name << "'" << std::endl;
          std::cout << sketch.sketch_.to_string();
          std::cout << std::string(80, '-') << std::endl;
        });
      }
    }
  }

  if (result.quantile_sketch.has_value()) {
    if (result.type == parquet::Type::BYTE_ARRAY || result.type == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
      auto quantiles = result.quantile_sketch->GetHistogramBounds<std::string>(10);
      std::cerr << name << " quantiles are " << quantiles << std::endl;
    } else {
      auto quantiles = result.quantile_sketch->GetHistogramBounds<int64_t>(10);
      std::cerr << name << " quantiles are " << quantiles << std::endl;
    }
  }

  if (result.frequent_items_sketch.has_value()) {
    if (result.type == parquet::Type::BYTE_ARRAY || result.type == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
      auto frequent_items = result.frequent_items_sketch->GetFrequentItems<std::string>(10);
      std::cerr << name << " frequent items are " << frequent_items << std::endl;
    } else {
      auto frequent_items = result.frequent_items_sketch->GetFrequentItems<int64_t>(10);
      std::cerr << name << " frequent items are " << frequent_items << std::endl;
    }
  }
}

void PrintAnalyzeResult(const AnalyzeResult& result, bool verbose = false) {
  for (const auto& [name, sketch] : result.sketches) {
    PrintAnalyzeColumnResult(name, sketch, verbose);
  }
}

}  // namespace stats

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

ABSL_FLAG(std::string, output_metadata_file, "", "");
ABSL_FLAG(std::string, filesystem, "local", "filesystem to use (local or s3)");
ABSL_FLAG(std::string, access_key_id, "", "s3 access key");
ABSL_FLAG(std::string, secret_key, "", "s3 secret key");
ABSL_FLAG(std::string, s3_endpoint, "", "s3 endpoint");
ABSL_FLAG(std::string, filename, "", "filename to process");
ABSL_FLAG(std::string, iceberg_metadata_location, "", "iceberg metadata location");
ABSL_FLAG(std::vector<std::string>, filenames, {}, "filenames to process");
ABSL_FLAG(bool, use_dictionary_optimization, true, "read data only from dictionary page if possible");
ABSL_FLAG(std::string, distinct_counter_implementation, "theta", "naive/hll/theta");
ABSL_FLAG(bool, use_theta_implementation, false, "use naive implementation");
ABSL_FLAG(bool, evaluate_quantiles, false, "evaluate quantiles");
ABSL_FLAG(bool, evaluate_frequent_items, false, "evaluate frequent items");
ABSL_FLAG(bool, verbose, false, "");
ABSL_FLAG(std::string, output_puffin_file, "", "");
ABSL_FLAG(int64_t, batch_size, 8192, "batch size");
ABSL_FLAG(std::vector<std::string>, columns_to_ignore, {}, "columns to ignore");

int main(int argc, char** argv) {
  std::optional<S3FinalizerGuard> s3_guard;

  try {
    absl::ParseCommandLine(argc, argv);

    const std::string filesystem_type = absl::GetFlag(FLAGS_filesystem);

    std::shared_ptr<arrow::fs::FileSystem> fs;
    if (filesystem_type == "local") {
      fs = std::make_shared<arrow::fs::LocalFileSystem>();
    } else if (filesystem_type == "s3") {
      s3_guard.emplace();
      const std::string access_key = absl::GetFlag(FLAGS_access_key_id);
      const std::string secret_key = absl::GetFlag(FLAGS_secret_key);
      auto s3options = arrow::fs::S3Options::FromAccessKey(access_key, secret_key);
      s3options.endpoint_override = absl::GetFlag(FLAGS_s3_endpoint);
      s3options.scheme = "http";
      auto maybe_fs = arrow::fs::S3FileSystem::Make(s3options);
      if (!maybe_fs.ok()) {
        std::cerr << maybe_fs.status() << std::endl;
        return 1;
      }
      fs = maybe_fs.MoveValueUnsafe();
    } else {
      throw arrow::Status::ExecutionError("Unexpected filesystem type");
    }

    const auto iceberg_metadata_location = absl::GetFlag(FLAGS_iceberg_metadata_location);

    std::vector<std::string> filenames = absl::GetFlag(FLAGS_filenames);
    const std::string filename = absl::GetFlag(FLAGS_filename);

    if (!iceberg_metadata_location.empty()) {
      auto maybe_meta = iceberg::ice_tea::GetScanMetadata(fs, iceberg_metadata_location);
      if (!maybe_meta.ok()) {
        std::cerr << maybe_meta.status() << std::endl;
        return 1;
      }
      auto scan_meta = maybe_meta.MoveValueUnsafe();
      for (const auto& partition : scan_meta.partitions) {
        for (const auto& layer : partition) {
          for (const auto& entry : layer.data_entries_) {
            std::string path = entry.path;
            if (path.starts_with("s3://")) {
              path = path.substr(std::string("s3://").size());
            }
            if (path.starts_with("s3a://")) {
              path = path.substr(std::string("s3a://").size());
            }
            if (path.starts_with("file://")) {
              path = path.substr(std::string("file://").size());
            }
            filenames.emplace_back(path);
          }
        }
      }
    }

    if (filename.empty() && filenames.empty()) {
      std::cerr << "filename is not set" << std::endl;
      return 1;
    }

    if (!filename.empty() && !filenames.empty()) {
      std::cerr << "filename and filenames cannot both be set at the same time" << std::endl;
      return 1;
    }

    if (!filename.empty()) {
      filenames.emplace_back(filename);
    }

    stats::Settings settings;

    settings.use_dictionary_optimization = absl::GetFlag(FLAGS_use_dictionary_optimization);

    const std::string distinct_counter_implementation_str = absl::GetFlag(FLAGS_distinct_counter_implementation);
    if (distinct_counter_implementation_str == "naive") {
      settings.distinct_counter_implementation = stats::DistinctCounterImplType::kNaive;
    } else if (distinct_counter_implementation_str == "theta") {
      settings.distinct_counter_implementation = stats::DistinctCounterImplType::kTheta;
    } else if (distinct_counter_implementation_str == "hll") {
      settings.distinct_counter_implementation = stats::DistinctCounterImplType::kHyperLogLog;
    }
    settings.evaluate_quantiles = absl::GetFlag(FLAGS_evaluate_quantiles);
    settings.evaluate_frequent_items = absl::GetFlag(FLAGS_evaluate_frequent_items);
    std::vector<std::string> columns_to_ginore = absl::GetFlag(FLAGS_columns_to_ignore);
    settings.columns_to_ignore = std::set<std::string>(columns_to_ginore.begin(), columns_to_ginore.end());
    settings.batch_size = absl::GetFlag(FLAGS_batch_size);
    settings.verbose = absl::GetFlag(FLAGS_verbose);
    settings.fs = fs;

    stats::Analyzer analyzer(settings);

    const auto& result = analyzer.Result();

    PrintAnalyzeResult(analyzer.Result(), settings.verbose);

    const std::string output_puffin_file = absl::GetFlag(FLAGS_output_puffin_file);
    if (!output_puffin_file.empty()) {
      iceberg::PuffinFileBuilder puffin_file_builder;

      for (const auto& [name, skethces] : result.sketches) {
        if (!skethces.counter) {
          continue;
        }
        const auto& impl = skethces.counter->GetState();
        if (auto ptr = std::get_if<stats::ThetaDistinctCounterWrapper>(&impl)) {
          auto data = ptr->Evaluate([&name](const stats::ThetaDistinctCounter& sketch) {
            datasketches::compact_theta_sketch final_sketch(sketch.sketch_, sketch.sketch_.is_ordered());
            std::stringstream ss;
            final_sketch.serialize(ss);
            return ss.str();
          });
          int64_t ndv = ptr->Evaluate(
              [&name](const stats::ThetaDistinctCounter& sketch) { return sketch.sketch_.get_estimate(); });
          std::map<std::string, std::string> properties;
          properties["ndv"] = std::to_string(ndv);
          puffin_file_builder.AppendBlob(std::move(data), std::move(properties), {skethces.field_id},
                                         "apache-datasketches-theta-v1");
        }
      }

      std::shared_ptr<iceberg::TableMetadataV2> iceberg_meta;
      if (!iceberg_metadata_location.empty()) {
        auto maybe_content = iceberg::ice_tea::ReadFile(fs, iceberg_metadata_location);
        if (!maybe_content.ok()) {
          std::cerr << maybe_content.status() << std::endl;
          return 1;
        }
        auto content = maybe_content.MoveValueUnsafe();

        auto meta = iceberg::ice_tea::ReadTableMetadataV2(content);
        if (!meta) {
          std::cerr << "Failed to read meta from iceberg" << std::endl;
          return 1;
        }
      }

      iceberg::PuffinFile puffin_result = [&]() {
        if (iceberg_meta) {
          puffin_file_builder.SetSequenceNumber(iceberg_meta->last_sequence_number);
          puffin_file_builder.SetSnapshotId(iceberg_meta->current_snapshot_id.value());
        } else {
          puffin_file_builder.SetSequenceNumber(-1);
          puffin_file_builder.SetSnapshotId(-1);
        }
        return std::move(puffin_file_builder).Build();
      }();

      auto maybe_output_stream = fs->OpenOutputStream(output_puffin_file);
      if (!maybe_output_stream.ok()) {
        std::cerr << maybe_output_stream.status() << std::endl;
        return 1;
      }
      auto output_stream = maybe_output_stream.MoveValueUnsafe();
      auto status = output_stream->Write(puffin_result.GetPayload());
      if (!status.ok()) {
        std::cerr << status << std::endl;
        return 1;
      }

      if (!iceberg_metadata_location.empty()) {
        auto footer = puffin_result.GetFooter();

        iceberg::Statistics stats;
        stats.statistics_path = "s3://" + output_puffin_file;
        stats.snapshot_id = iceberg_meta->current_snapshot_id.value();
        stats.file_footer_size_in_bytes = footer.GetPayload().size();
        stats.file_size_in_bytes = puffin_result.GetPayload().size();

        auto footer_info = footer.GetDeserializedFooter();
        for (const auto& blob : footer_info.blobs) {
          iceberg::BlobMetadata result_blob;
          result_blob.field_ids = blob.fields;
          result_blob.properties = blob.properties;
          result_blob.sequence_number = blob.sequence_number;
          result_blob.type = blob.type;
          result_blob.snapshot_id = blob.snapshot_id;

          stats.blob_metadata.emplace_back(std::move(result_blob));
        }

        iceberg_meta->statistics.emplace_back(stats);

        std::string res = iceberg::ice_tea::WriteTableMetadataV2(*iceberg_meta, true);

        const std::string output_metadata_file = absl::GetFlag(FLAGS_output_metadata_file);
        iceberg::ice_tea::WriteMetadataFileRemote(fs, output_metadata_file, iceberg_meta);
      }
    }
    return 0;
  } catch (arrow::Status& s) {
    std::cerr << s.ToString() << std::endl;
    return 1;
  } catch (std::exception& e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }
}
