#include <iostream>
#include <stdexcept>
#include <string>
#include <variant>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "parquet/column_reader.h"
#include "parquet/file_reader.h"
#include "parquet/types.h"
#include "stats/parquet/distinct.h"
#include "stats/parquet/frequent_item.h"
#include "stats/parquet/quantiles.h"
#include "stats/types.h"

class ReaderHelper {
 private:
  template <typename Type>
  void PrepareDataBuffer() {
    data_buffer_ = std::make_unique<Type>(batch_size_);
  }

  template <typename ParquetReaderType, typename DataType>
  std::pair<void*, uint64_t> ReadData(std::shared_ptr<parquet::ColumnReader> reader) {
    int64_t values_read;
    auto typed_reader = std::static_pointer_cast<ParquetReaderType>(reader);
    auto& data_uptr = std::get<UPtr<DataType>>(data_buffer_);
    typed_reader->ReadBatch(batch_size_, def_levels_.get(), rep_levels_.get(), data_uptr.get(), &values_read);
    return std::make_pair(data_uptr.get(), values_read);
  }

 public:
  ReaderHelper(uint32_t batch_size, parquet::Type::type type) : batch_size_(batch_size), type_(type) {
    def_levels_ = std::make_unique<int16_t[]>(batch_size);
    rep_levels_ = std::make_unique<int16_t[]>(batch_size);
    switch (type_) {
      case parquet::Type::INT32:
        PrepareDataBuffer<Int32Data>();
        break;
      case parquet::Type::INT64:
        PrepareDataBuffer<Int64Data>();
        break;
      case parquet::Type::FIXED_LEN_BYTE_ARRAY:
        PrepareDataBuffer<FLBAData>();
        break;
      case parquet::Type::BYTE_ARRAY:
        PrepareDataBuffer<ByteArrayData>();
        break;
      default:
        throw std::runtime_error("Unsupported type " + std::to_string(static_cast<int>(type_)));
    }
  }

  std::pair<void*, uint64_t> ReadData(std::shared_ptr<parquet::ColumnReader> reader) {
    switch (type_) {
      case parquet::Type::INT32:
        return ReadData<parquet::Int32Reader, Int32Data>(reader);
      case parquet::Type::INT64:
        return ReadData<parquet::Int64Reader, Int64Data>(reader);
      case parquet::Type::FIXED_LEN_BYTE_ARRAY:
        return ReadData<parquet::FixedLenByteArrayReader, FLBAData>(reader);
      case parquet::Type::BYTE_ARRAY:
        return ReadData<parquet::ByteArrayReader, ByteArrayData>(reader);
      default:
        throw std::runtime_error("Unsupported type " + std::to_string(static_cast<int>(type_)));
    }
    return {nullptr, 0};
  }

 private:
  const uint32_t batch_size_;
  const parquet::Type::type type_;

  using Int16Data = int16_t[];
  using Int32Data = int32_t[];
  using Int64Data = int64_t[];
  using FLBAData = parquet::FixedLenByteArray[];
  using ByteArrayData = parquet::ByteArray[];

  template <typename T>
  using UPtr = std::unique_ptr<T>;

  UPtr<Int16Data> def_levels_;
  UPtr<Int16Data> rep_levels_;

  std::variant<UPtr<Int32Data>, UPtr<Int64Data>, UPtr<FLBAData>, UPtr<ByteArrayData>> data_buffer_;
};

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

ABSL_FLAG(std::string, filename, "", "filename to process");
ABSL_FLAG(bool, use_dictionary_optimization, true, "read data only from dictionary page if possible");
ABSL_FLAG(bool, use_naive_implementation, false, "use naive implementation");
ABSL_FLAG(bool, evaluate_quantiles, false, "evaluate quantiles");
ABSL_FLAG(bool, evaluate_frequent_items, false, "evaluate frequent items");
ABSL_FLAG(std::vector<std::string>, columns_to_ignore, {}, "columns to ignore");

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  const std::string filename = absl::GetFlag(FLAGS_filename);
  if (filename.empty()) {
    std::cerr << "filename is not set" << std::endl;
    return 1;
  }

  const bool use_dictionary_optimization = absl::GetFlag(FLAGS_use_dictionary_optimization);
  const bool use_naive_implementation = absl::GetFlag(FLAGS_use_naive_implementation);
  const bool evaluate_quantiles = absl::GetFlag(FLAGS_evaluate_quantiles);
  const bool evaluate_frequent_items = absl::GetFlag(FLAGS_evaluate_frequent_items);
  const std::vector<std::string> columns_to_ignore = absl::GetFlag(FLAGS_columns_to_ignore);

  auto file_reader = parquet::ParquetFileReader::OpenFile(filename);
  auto metadata = file_reader->metadata();
  auto num_row_groups = metadata->num_row_groups();
  auto num_columns = metadata->num_columns();
  std::cerr << "num_row_groups = " << num_row_groups << std::endl;
  std::cerr << "num_columns = " << num_columns << std::endl;

  std::map<std::string, parquet::Type::type> types;
  std::map<std::string, ReaderHelper> reader_helpers;
  std::map<std::string, stats::CommonDistinctWrapper> counters;
  std::map<std::string, stats::CommonQuantileWrapper> quantiles_sketches;
  std::map<std::string, stats::CommonFrequentItemsWrapper> frequent_items_sketches;

  constexpr int32_t kBatchSize = 8192;

  for (auto rg = 0; rg < num_row_groups; ++rg) {
    auto rg_reader = file_reader->RowGroup(rg);
    auto rg_metadata = rg_reader->metadata();
    for (auto col = 0; col < num_columns; ++col) {
      auto col_metadata = rg_metadata->ColumnChunk(col);
      auto col_name = metadata->schema()->Column(col)->name();
      if (std::find(columns_to_ignore.begin(), columns_to_ignore.end(), col_name) != columns_to_ignore.end()) {
        continue;
      }
      auto type = col_metadata->type();

      if (rg == 0 || col == 0) {
        types.emplace(col_name, type);
        if (use_naive_implementation) {
          counters.emplace(col_name, stats::NaiveDistinctCounterWrapper());
        } else {
          const bool is_string_type = type == parquet::Type::FIXED_LEN_BYTE_ARRAY || type == parquet::Type::BYTE_ARRAY;
          const auto stats_type = is_string_type ? stats::Type::kString : stats::Type::kInt64;
          frequent_items_sketches.emplace(col_name, stats::CommonFrequentItemsWrapper(stats_type));
          quantiles_sketches.emplace(col_name, stats::CommonQuantileWrapper(stats_type));
          counters.emplace(col_name, stats::HLLDistinctCounterWrapper());
        }
        if (type == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
          auto len = rg_metadata->schema()->Column(col)->type_length();
          counters.at(col_name).SetFBLALength(len);
        }
        reader_helpers.emplace(col_name, ReaderHelper(kBatchSize, type));
      }

      auto& counter_for_col = counters.at(col_name);

      if (use_dictionary_optimization && !evaluate_quantiles && !evaluate_frequent_items) {
        auto encoding_stats = col_metadata->encoding_stats();
        bool only_dict_encoding = col_metadata->has_dictionary_page();
        for (auto stat : encoding_stats) {
          if (stat.page_type == parquet::PageType::DATA_PAGE || stat.page_type == parquet::PageType::DATA_PAGE_V2) {
            if (stat.encoding != parquet::Encoding::PLAIN_DICTIONARY &&
                stat.encoding != parquet::Encoding::RLE_DICTIONARY) {
              only_dict_encoding = false;
            }
          }
        }

        if (only_dict_encoding) {
          auto record_reader = rg_reader->RecordReader(col, true);
          int32_t dict_len;
          auto* dict_data = record_reader->ReadDictionary(&dict_len);
          counter_for_col.AppendValues(dict_data, dict_len, type);
          continue;
        }
      }

      int64_t total_values = col_metadata->num_values();
      int64_t values_read = 0;
      auto col_reader = rg_reader->Column(col);
      auto& reader_helper = reader_helpers.at(col_name);

      while (values_read < total_values) {
        auto [data_ptr, data_size] = reader_helper.ReadData(col_reader);
        counter_for_col.AppendValues(data_ptr, data_size, type);
        if (evaluate_quantiles) {
          auto& quantiles_for_col = quantiles_sketches.at(col_name);
          quantiles_for_col.AppendValues(data_ptr, data_size, type);
        }

        if (evaluate_frequent_items) {
          auto& frequent_items_for_col = frequent_items_sketches.at(col_name);
          frequent_items_for_col.AppendValues(data_ptr, data_size, type);
        }
        values_read += data_size;
      }
    }
  }

  for (const auto& [col_name, counter] : counters) {
    std::cerr << col_name << " has " << counter.GetNumberOfDistinctValues() << " distinct values " << std::endl;
  }
  if (evaluate_quantiles) {
    for (const auto& [col_name, quantile_sketch] : quantiles_sketches) {
      if (types.at(col_name) == parquet::Type::BYTE_ARRAY ||
          types.at(col_name) == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
        auto quantiles = quantile_sketch.GetHistogramBounds<std::string>(10);
        std::cerr << col_name << " quantiles are " << quantiles << std::endl;
      } else {
        auto quantiles = quantile_sketch.GetHistogramBounds<int64_t>(10);
        std::cerr << col_name << " quantiles are " << quantiles << std::endl;
      }
    }
  }

  if (evaluate_frequent_items) {
    for (const auto& [col_name, frequent_items_sketch] : frequent_items_sketches) {
      if (types.at(col_name) == parquet::Type::BYTE_ARRAY ||
          types.at(col_name) == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
        auto frequent_items = frequent_items_sketch.GetFrequentItems<std::string>(10);
        std::cerr << col_name << " frequent items are " << frequent_items << std::endl;
      } else {
        auto frequent_items = frequent_items_sketch.GetFrequentItems<int64_t>(10);
        std::cerr << col_name << " frequent items are " << frequent_items << std::endl;
      }
    }
  }
  return 0;
}
