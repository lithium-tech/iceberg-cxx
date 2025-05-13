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
#include "iceberg/common/fs/filesystem_wrapper.h"
#include "iceberg/puffin.h"
#include "iceberg/result.h"
#include "iceberg/table_metadata.h"
#include "iceberg/tea_scan.h"
#include "iceberg/write.h"
#include "stats/analyzer.h"
#include "stats/datasketch/distinct_theta.h"
#include "stats/datasketch/frequent_items.h"
#include "stats/datasketch/quantiles.h"
#include "stats/puffin.h"
#include "stats/types.h"

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

struct Metrics {
  uint64_t requests = 0;
  uint64_t bytes_read = 0;
  uint64_t files_opened = 0;
};

class LoggingInputFile : public iceberg::InputFileWrapper {
 public:
  LoggingInputFile(std::shared_ptr<arrow::io::RandomAccessFile> file, std::shared_ptr<Metrics> metrics)
      : InputFileWrapper(file), metrics_(metrics) {}

  arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override {
    TakeRequestIntoAccount(nbytes);
    return InputFileWrapper::ReadAt(position, nbytes, out);
  }

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override {
    TakeRequestIntoAccount(nbytes);
    return InputFileWrapper::Read(nbytes, out);
  }

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override {
    TakeRequestIntoAccount(nbytes);
    return InputFileWrapper::Read(nbytes);
  }

 private:
  void TakeRequestIntoAccount(int64_t bytes) {
    ++metrics_->requests;
    metrics_->bytes_read += bytes;
  }

  std::shared_ptr<Metrics> metrics_;
};

class LoggingFileSystem : public iceberg::FileSystemWrapper {
 public:
  LoggingFileSystem(std::shared_ptr<arrow::fs::FileSystem> fs, std::shared_ptr<Metrics> metrics)
      : FileSystemWrapper(fs), metrics_(metrics) {}

  arrow::Result<std::shared_ptr<arrow::io::RandomAccessFile>> OpenInputFile(const std::string& path) override {
    ++metrics_->files_opened;
    ARROW_ASSIGN_OR_RAISE(auto file, FileSystemWrapper::OpenInputFile(path));
    return std::make_shared<LoggingInputFile>(file, metrics_);
  }

 private:
  std::shared_ptr<Metrics> metrics_;
};

namespace stats {

void PrintQuantile(const std::string& name, const std::optional<GenericQuantileSketch>& quantile) {
  if (quantile.has_value() && !quantile->Empty()) {
    if (quantile->Type() == stats::Type::kString) {
      auto quantiles = quantile->GetHistogramBounds<std::string>(10);
      std::cerr << name << " quantiles are " << quantiles << std::endl;
    } else {
      auto quantiles = quantile->GetHistogramBounds<int64_t>(10);
      std::cerr << name << " quantiles are " << quantiles << std::endl;
    }
  }
}

void PrintFrequent(const std::string& name, const std::optional<GenericFrequentItemsSketch>& quantile) {
  if (quantile.has_value() && !quantile->Empty()) {
    if (quantile->Type() == stats::Type::kString) {
      auto frequent_items = quantile->GetFrequentItems<std::string>();
      std::cerr << name << " frequent items are " << frequent_items << std::endl;
    } else {
      auto frequent_items = quantile->GetFrequentItems<int64_t>();
      std::cerr << name << " frequent items are " << frequent_items << std::endl;
    }
  }
}

void PrintAnalyzeColumnResult(const std::string& name, const AnalyzeColumnResult& result, bool verbose = false) {
  if (result.counter.has_value()) {
    std::cerr << name << " has " << result.counter->GetDistinctValuesCount() << " distinct values" << std::endl;

    if (verbose) {
      const auto& impl = result.counter->GetSketch();
      if (auto ptr = std::get_if<stats::ThetaDistinctCounter>(&impl)) {
        std::cout << "Verbose state for column '" << name << "'" << std::endl;
        std::cout << ptr->GetSketch().to_string();
        std::cout << std::string(80, '-') << std::endl;
      }
    }
  }

  PrintQuantile(name, result.quantile_sketch);
  PrintQuantile(name, result.quantile_sketch_dictionary);

  PrintFrequent(name, result.frequent_items_sketch);
  PrintFrequent(name, result.frequent_items_sketch_dictionary);
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
ABSL_FLAG(bool, use_precalculation_optimization, true, "");
ABSL_FLAG(bool, use_string_view_heuristic, true, "");
ABSL_FLAG(std::optional<int>, row_groups_limit, std::nullopt, "");
ABSL_FLAG(std::string, distinct_counter_implementation, "theta", "naive/hll/theta");
ABSL_FLAG(bool, evaluate_distinct, false, "evaluate distinct values");
ABSL_FLAG(bool, evaluate_quantiles, false, "evaluate quantiles");
ABSL_FLAG(bool, evaluate_frequent_items, false, "evaluate frequent items");
ABSL_FLAG(bool, verbose, false, "");
ABSL_FLAG(bool, print_timings, false, "");
ABSL_FLAG(bool, read_all_data, false, "");
ABSL_FLAG(std::string, output_puffin_file, "", "");
ABSL_FLAG(int64_t, batch_size, 8192, "batch size");
ABSL_FLAG(std::vector<std::string>, columns_to_ignore, {}, "columns to ignore");
ABSL_FLAG(std::vector<std::string>, columns_to_process, {}, "columns to process");

int main(int argc, char** argv) {
  std::shared_ptr<Metrics> metrics = std::make_shared<Metrics>();
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
      fs = iceberg::ValueSafe(arrow::fs::S3FileSystem::Make(s3options));
    } else {
      throw arrow::Status::ExecutionError("Unexpected filesystem type");
    }

    fs = std::make_shared<LoggingFileSystem>(fs, metrics);

    const auto iceberg_metadata_location = absl::GetFlag(FLAGS_iceberg_metadata_location);

    std::vector<std::string> filenames = absl::GetFlag(FLAGS_filenames);
    const std::string filename = absl::GetFlag(FLAGS_filename);

    if (!iceberg_metadata_location.empty()) {
      auto scan_meta = iceberg::ValueSafe(iceberg::ice_tea::GetScanMetadata(fs, iceberg_metadata_location));
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
    settings.use_precalculation_optimization = absl::GetFlag(FLAGS_use_precalculation_optimization);
    settings.use_string_view_heuristic = absl::GetFlag(FLAGS_use_string_view_heuristic);
    settings.row_groups_limit = absl::GetFlag(FLAGS_row_groups_limit);

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
    std::vector<std::string> columns_to_process = absl::GetFlag(FLAGS_columns_to_process);
    settings.columns_to_process = std::set<std::string>(columns_to_process.begin(), columns_to_process.end());

    settings.batch_size = absl::GetFlag(FLAGS_batch_size);
    settings.verbose = absl::GetFlag(FLAGS_verbose);
    settings.print_timings = absl::GetFlag(FLAGS_print_timings);
    settings.fs = fs;
    settings.evaluate_distinct = absl::GetFlag(FLAGS_evaluate_distinct);
    settings.read_all_data = absl::GetFlag(FLAGS_read_all_data);

    stats::Analyzer analyzer(settings);
    for (auto& name : filenames) {
      analyzer.Analyze(name);
    }

    const auto& result = analyzer.Result();

    PrintAnalyzeResult(result, settings.verbose);

    std::cerr << "bytes_read = " << metrics->bytes_read << std::endl;
    std::cerr << "requests = " << metrics->requests << std::endl;

    const std::string output_puffin_file = absl::GetFlag(FLAGS_output_puffin_file);

    if (!output_puffin_file.empty()) {
      std::shared_ptr<iceberg::TableMetadataV2> iceberg_meta;
      if (!iceberg_metadata_location.empty()) {
        auto content = iceberg::ValueSafe(iceberg::ice_tea::ReadFile(fs, iceberg_metadata_location));
        auto meta = iceberg::ice_tea::ReadTableMetadataV2(content);
        if (!meta) {
          std::cerr << "Failed to read meta from iceberg" << std::endl;
          return 1;
        }
      }

      iceberg::PuffinFileBuilder puffin_file_builder;
      stats::SketchesToPuffin(result, puffin_file_builder);

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

      auto output_stream = iceberg::ValueSafe(fs->OpenOutputStream(output_puffin_file));
      iceberg::Ensure(output_stream->Write(puffin_result.GetPayload()));

      if (!iceberg_metadata_location.empty()) {
        iceberg::Statistics stats = stats::PuffinInfoToStatistics(puffin_result, "s3://" + output_puffin_file,
                                                                  iceberg_meta->current_snapshot_id.value());

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
