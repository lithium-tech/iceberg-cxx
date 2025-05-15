#include <chrono>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>

#define ENABLE_PRIVATE_PUBLIC_HACK

#ifdef ENABLE_PRIVATE_PUBLIC_HACK
#define private public
#endif
#include "iceberg/result.h"
#include "iceberg/streams/arrow/error.h"
#include "parquet/column_page.h"
#include "parquet/column_reader.h"
#include "parquet/encoding.h"
#include "parquet/file_reader.h"
#include "parquet/metadata.h"
#include "parquet/properties.h"
#include "parquet/schema.h"
#include "parquet/type_fwd.h"
#include "parquet/types.h"
#include "stats/analyzer.h"
#include "stats/data.h"
#include "stats/datasketch/distinct.h"
#include "stats/datasketch/frequent_items.h"
#include "stats/datasketch/quantiles.h"
#include "stats/datasketch/string_view_serializer.h"
#include "stats/measure.h"
#include "stats/parquet/sketch_updater.h"
#include "stats/parquet/values_provider.h"
#include "stats/types.h"

namespace stats {
namespace {

template <ParquetType parquet_type>
GenericParquetReader MakeGenericParquetReader(const GenericParquetBuffer& buffer,
                                              std::shared_ptr<parquet::ColumnReader> col_reader, int batch_size) {
  auto typed_buffer = buffer.Get<parquet_type>();
  auto typed_reader =
      std::static_pointer_cast<typename parquet::TypedColumnReader<parquet::PhysicalType<parquet_type>>>(col_reader);
  auto result_reader = std::make_shared<ParquetReader<parquet_type>>(typed_reader, typed_buffer, batch_size);
  return GenericParquetReader(result_reader);
}

template <ParquetType parquet_type>
GenericParquetDictionaryReader MakeGenericParquetDictionaryReader(const GenericParquetDictionaryBuffer& buffer,
                                                                  std::shared_ptr<parquet::ColumnReader> col_reader,
                                                                  int batch_size) {
  auto typed_buffer = buffer.Get<parquet_type>();
  auto typed_reader =
      std::static_pointer_cast<typename parquet::TypedColumnReader<parquet::PhysicalType<parquet_type>>>(col_reader);
  auto result_reader = std::make_shared<ParquetDictionaryReader<parquet_type>>(typed_reader, typed_buffer, batch_size);
  return GenericParquetDictionaryReader(result_reader);
}

std::optional<GenericParquetReader> MakeGenericParquetReaderFromType(const GenericParquetBuffer& buffer,
                                                                     std::shared_ptr<parquet::ColumnReader> col_reader,
                                                                     int batch_size, ParquetType parquet_type) {
  switch (parquet_type) {
    case parquet::Type::INT32:
      return MakeGenericParquetReader<ParquetType::INT32>(buffer, col_reader, batch_size);
    case parquet::Type::INT64:
      return MakeGenericParquetReader<ParquetType::INT64>(buffer, col_reader, batch_size);
    case parquet::Type::BYTE_ARRAY:
      return MakeGenericParquetReader<ParquetType::BYTE_ARRAY>(buffer, col_reader, batch_size);
    // not supported currently
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
    case parquet::Type::INT96:
    case parquet::Type::FLOAT:
    case parquet::Type::DOUBLE:
    case parquet::Type::BOOLEAN:
    case parquet::Type::UNDEFINED:
      return std::nullopt;
  }
  throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": internal error");
}

std::optional<GenericParquetDictionaryReader> MakeGenericParquetDictionaryReaderFromType(
    const GenericParquetDictionaryBuffer& buffer, std::shared_ptr<parquet::ColumnReader> col_reader, int batch_size,
    ParquetType parquet_type) {
  switch (parquet_type) {
    case parquet::Type::INT32:
      return MakeGenericParquetDictionaryReader<ParquetType::INT32>(buffer, col_reader, batch_size);
    case parquet::Type::INT64:
      return MakeGenericParquetDictionaryReader<ParquetType::INT64>(buffer, col_reader, batch_size);
    case parquet::Type::BYTE_ARRAY:
      return MakeGenericParquetDictionaryReader<ParquetType::BYTE_ARRAY>(buffer, col_reader, batch_size);
    // not supported currently
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
    case parquet::Type::INT96:
    case parquet::Type::FLOAT:
    case parquet::Type::DOUBLE:
    case parquet::Type::BOOLEAN:
    case parquet::Type::UNDEFINED:
      return std::nullopt;
  }
  throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": internal error");
}

std::optional<GenericParquetBuffer> MakeBufferFromType(int32_t batch_size, ParquetType type) {
  switch (type) {
    case parquet::Type::INT32: {
      auto buffer = std::make_shared<Int32ParquetBuffer>(batch_size);
      return GenericParquetBuffer(buffer);
    }
    case parquet::Type::INT64: {
      auto buffer = std::make_shared<Int64ParquetBuffer>(batch_size);
      return GenericParquetBuffer(buffer);
    }
    case parquet::Type::BYTE_ARRAY: {
      auto buffer = std::make_shared<ByteArrayParquetBuffer>(batch_size);
      return GenericParquetBuffer(buffer);
    }
    // not supported currently
    case parquet::Type::INT96:
    case parquet::Type::FLOAT:
    case parquet::Type::DOUBLE:
    case parquet::Type::BOOLEAN:
    case parquet::Type::UNDEFINED:
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      return std::nullopt;
  }
  throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": internal error");
}

std::optional<GenericParquetDictionaryBuffer> MakeDictionaryBufferFromType(int32_t batch_size, ParquetType type) {
  switch (type) {
    case parquet::Type::INT32: {
      auto buffer = std::make_shared<Int32ParquetDictionaryBuffer>(batch_size);
      return GenericParquetDictionaryBuffer(buffer);
    }
    case parquet::Type::INT64: {
      auto buffer = std::make_shared<Int64ParquetDictionaryBuffer>(batch_size);
      return GenericParquetDictionaryBuffer(buffer);
    }
    case parquet::Type::BYTE_ARRAY: {
      auto buffer = std::make_shared<ByteArrayParquetDictionaryBuffer>(batch_size);
      return GenericParquetDictionaryBuffer(buffer);
    }
      // not supported currently
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
    case parquet::Type::INT96:
    case parquet::Type::FLOAT:
    case parquet::Type::DOUBLE:
    case parquet::Type::BOOLEAN:
    case parquet::Type::UNDEFINED:
      return std::nullopt;
  }
  throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": internal error");
}

GenericDistinctCounterSketch MakeDistinctCounter(DistinctCounterImplType type) {
  switch (type) {
    case DistinctCounterImplType::kNaive:
      return GenericDistinctCounterSketch(stats::NaiveDistinctCounter());
    case DistinctCounterImplType::kTheta:
      return GenericDistinctCounterSketch(stats::ThetaDistinctCounter());
    case DistinctCounterImplType::kHyperLogLog:
      return GenericDistinctCounterSketch(stats::HLLDistinctCounter());
  }
  throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": internal error");
}

GenericQuantileSketch MakeQuantileCounter(parquet::Type::type type) {
  const bool is_string_type = type == parquet::Type::FIXED_LEN_BYTE_ARRAY || type == parquet::Type::BYTE_ARRAY;
  const auto stats_type = is_string_type ? stats::Type::kString : stats::Type::kInt64;

  return GenericQuantileSketch(stats_type);
}

GenericFrequentItemsSketch MakeFrequentItemsCounter(parquet::Type::type type) {
  const bool is_string_type = type == parquet::Type::FIXED_LEN_BYTE_ARRAY || type == parquet::Type::BYTE_ARRAY;
  const auto stats_type = is_string_type ? stats::Type::kString : stats::Type::kInt64;

  return GenericFrequentItemsSketch(stats_type);
}

AnalyzeColumnResult InitializeSketchesForColumn(const parquet::ColumnDescriptor& descriptor, const Settings& settings) {
  AnalyzeColumnResult column_result;
  auto type = descriptor.physical_type();

  column_result.type = type;
  column_result.field_id = descriptor.schema_node()->field_id();
  if (settings.evaluate_distinct) {
    column_result.distinct = MakeDistinctCounter(settings.distinct_counter_implementation);
  }
  if (settings.evaluate_quantiles) {
    column_result.quantile_sketch.emplace(MakeQuantileCounter(type));
    if (settings.use_dictionary_optimization) {
      column_result.quantile_sketch_dictionary.emplace(MakeQuantileCounter(parquet::Type::INT32));
    }
  }
  if (settings.evaluate_frequent_items) {
    column_result.frequent_items_sketch.emplace(MakeFrequentItemsCounter(type));
    if (settings.use_dictionary_optimization) {
      column_result.frequent_items_sketch_dictionary.emplace(MakeFrequentItemsCounter(parquet::Type::INT32));
    }
  }

  return column_result;
}

bool IsDictionaryEncoding(parquet::Encoding::type type) {
  switch (type) {
    case parquet::Encoding::PLAIN_DICTIONARY:
    case parquet::Encoding::RLE_DICTIONARY:
      return true;
    case parquet::Encoding::PLAIN:
    case parquet::Encoding::RLE:
    case parquet::Encoding::BIT_PACKED:
    case parquet::Encoding::DELTA_BINARY_PACKED:
    case parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY:
    case parquet::Encoding::DELTA_BYTE_ARRAY:
    case parquet::Encoding::BYTE_STREAM_SPLIT:
    case parquet::Encoding::UNDEFINED:
    case parquet::Encoding::UNKNOWN:
      return false;
  }
  throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": internal error");
}

bool AllDataPagesAreEncodedWithDictionary(const parquet::ColumnChunkMetaData& column_metadata) {
  auto encoding_stats = column_metadata.encoding_stats();

  if (!column_metadata.has_dictionary_page()) {
    return false;
  }

  // check that all data pages are encoded with dictionary encoding
  for (auto stat : encoding_stats) {
    switch (stat.page_type) {
      case parquet::PageType::INDEX_PAGE:
      case parquet::PageType::UNDEFINED:
        return false;
      case parquet::PageType::DATA_PAGE:
      case parquet::PageType::DATA_PAGE_V2:
        if (!IsDictionaryEncoding(stat.encoding)) {
          return false;
        }
      case parquet::PageType::DICTIONARY_PAGE:
        break;
    }
  }

  return true;
}

template <typename DType>
const void* ReadDictionary(std::shared_ptr<parquet::DictionaryPage> dictionary_page,
                           const parquet::ColumnDescriptor* descr, int32_t* dictionary_length,
                           std::unique_ptr<parquet::DictDecoder<DType>>& decoder) {
  auto dictionary = MakeTypedDecoder<DType>(parquet::Encoding::PLAIN, descr);
  dictionary->SetData(dictionary_page->num_values(), dictionary_page->data(), dictionary_page->size());

  decoder = MakeDictDecoder<DType>(descr);
  decoder->SetDict(dictionary.get());

  const typename DType::c_type* data;
  decoder->GetDictionary(&data, dictionary_length);

  return reinterpret_cast<const void*>(data);
}
}  // namespace

bool Analyzer::EvaluateStatsFromDictionaryPage(const parquet::RowGroupMetaData& rg_metadata, int col,
                                               parquet::RowGroupReader& rg_reader) {
  auto col_metadata = rg_metadata.ColumnChunk(col);
  iceberg::Ensure(col_metadata != nullptr, "No column metadata for column " + std::to_string(col));

  if (!AllDataPagesAreEncodedWithDictionary(*col_metadata)) {
    return false;
  }

  auto descr = rg_metadata.schema()->Column(col);
  auto col_name = rg_metadata.schema()->Column(col)->name();

  auto& column_result = result_.sketches.at(col_name);
  auto& counter_for_col = *column_result.distinct;
  auto type = col_metadata->type();

#ifdef ENABLE_PRIVATE_PUBLIC_HACK
  auto page_reader = rg_reader.contents_->GetColumnPageReader(col);
  iceberg::Ensure(page_reader != nullptr, "No page_reader for col " + std::to_string(col));

  auto page = page_reader->NextPage();
  iceberg::Ensure(page != nullptr, "No page for col " + std::to_string(col));

  iceberg::Ensure(page->type() == parquet::PageType::DICTIONARY_PAGE,
                  "Expected dictionary page fol col " + std::to_string(col));
  auto dictionary_page = std::static_pointer_cast<parquet::DictionaryPage>(page);

  iceberg::Ensure(dictionary_page->encoding() == parquet::Encoding::PLAIN ||
                      dictionary_page->encoding() == parquet::Encoding::PLAIN_DICTIONARY,
                  "Expected dictionary page fol col " + std::to_string(col) + ", but found page with type " +
                      std::to_string(dictionary_page->encoding()));

  if (type == parquet::Type::INT32) {
    int32_t dict_len;

    std::unique_ptr<parquet::DictDecoder<parquet::Int32Type>> decoder;
    const void* dict_data = ReadDictionary<parquet::Int32Type>(dictionary_page, descr, &dict_len, decoder);

    iceberg::ScopedTimerClock counter_timer(metrics_per_column_[col_name].distinct_);
    SketchUpdater updater(counter_for_col);
    updater.AppendValues(dict_data, dict_len, type);
  } else if (type == parquet::Type::INT64) {
    int32_t dict_len;

    std::unique_ptr<parquet::DictDecoder<parquet::Int64Type>> decoder;
    const void* dict_data = ReadDictionary<parquet::Int64Type>(dictionary_page, descr, &dict_len, decoder);

    iceberg::ScopedTimerClock counter_timer(metrics_per_column_[col_name].distinct_);
    SketchUpdater updater(counter_for_col);
    updater.AppendValues(dict_data, dict_len, type);
  } else if (type == parquet::Type::BYTE_ARRAY) {
    int32_t dict_len;

    std::unique_ptr<parquet::DictDecoder<parquet::ByteArrayType>> decoder;
    const void* dict_data = ReadDictionary<parquet::ByteArrayType>(dictionary_page, descr, &dict_len, decoder);

    iceberg::ScopedTimerClock counter_timer(metrics_per_column_[col_name].distinct_);
    SketchUpdater updater(counter_for_col);
    updater.AppendValues(dict_data, dict_len, type);
  } else {
    iceberg::Ensure(false,
                    "Unexpected type " + std::to_string(static_cast<int>(type)) + " for col " + std::to_string(col));
  }

#else
  auto record_reader = rg_reader.RecordReaderWithExposeEncoding(col, parquet::ExposedEncoding::DICTIONARY);
  std::optional<iceberg::ScopedTimerClock> read_timer(metrics_per_column_[col_name].reading_);
  int32_t dict_len;
  auto* dict_data = record_reader->ReadDictionary(&dict_len);
  read_timer.reset();

  iceberg::ScopedTimerClock counter_timer(metrics_per_column_[col_name].distinct_);
  SketchUpdater updater(counter_for_col);
  updater.AppendValues(dict_data, dict_len, type);
#endif
  return true;
}

void Analyzer::AnalyzeColumn(const parquet::RowGroupMetaData& rg_metadata, int col,
                             parquet::RowGroupReader& rg_reader) {
  auto col_metadata = rg_metadata.ColumnChunk(col);
  iceberg::Ensure(col_metadata != nullptr, "No column metadata for column " + std::to_string(col));

  auto descr = rg_metadata.schema()->Column(col);
  auto col_name = descr->name();
  auto type = col_metadata->type();

  auto& column_result = result_.sketches.at(col_name);

  auto& counter_for_col = *column_result.distinct;

  int64_t total_values = col_metadata->num_values();
  int64_t values_read = 0;

  if (settings_.use_dictionary_optimization && AllDataPagesAreEncodedWithDictionary(*col_metadata)) {
    ++metrics_per_column_[col_name].dictionary_encoded_count_;
    bool distinct_is_evaluated = false;
    if (settings_.evaluate_distinct) {
      distinct_is_evaluated = EvaluateStatsFromDictionaryPage(rg_metadata, col, rg_reader);
      iceberg::Ensure(distinct_is_evaluated, "Column " + col_name + " is not completely dictionary encoded");
    }

    if (!settings_.evaluate_frequent_items && !settings_.evaluate_quantiles && !settings_.read_all_data) {
      return;
    }

    auto col_reader = rg_reader.ColumnWithExposeEncoding(col, parquet::ExposedEncoding::DICTIONARY);
    iceberg::Ensure(col_reader->GetExposedEncoding() == parquet::ExposedEncoding::DICTIONARY,
                    "Column " + col_name + " is not completely dictionary encoded");

    const auto& dictionary_buffer = dictionary_buffers.at(col_name);
    auto& count_buffer = count_buffers[col_name];
    count_buffer.clear();

    std::optional<GenericParquetDictionaryReader> reader =
        MakeGenericParquetDictionaryReaderFromType(dictionary_buffer, col_reader, settings_.batch_size, type);
    iceberg::Ensure(reader.has_value(), "Failed to constructor reader for column " + col_name);

    while (true) {
      std::optional<iceberg::ScopedTimerClock> read_timer(metrics_per_column_[col_name].reading_);
      if (!col_reader->HasNext()) {
        break;
      }
      auto generic_buffer = reader->ReadGeneric();
      read_timer.reset();

      // distinct is already evaluated from dictionary page

      if (settings_.use_precalculation_optimization) {
        iceberg::ScopedTimerClock quantile_timer(metrics_per_column_[col_name].counting_);
        if (count_buffer.size() == 0) {
          count_buffer.resize(generic_buffer.dictionary_length, 0);
        }

        for (int i = 0; i < generic_buffer.values_size; ++i) {
          ++count_buffer[generic_buffer.indices[i]];
        }
      }

      if (settings_.evaluate_frequent_items && !settings_.use_precalculation_optimization) {
        iceberg::ScopedTimerClock frequent_timer(metrics_per_column_[col_name].frequent_items_);
        auto& frequent_items_for_col = *column_result.frequent_items_sketch_dictionary;
        SketchUpdater updater(frequent_items_for_col);
        frequent_items_updates_ += generic_buffer.values_size;
        updater.AppendValues(generic_buffer.indices, generic_buffer.values_size, parquet::Type::INT32);
      }
      values_read += generic_buffer.values_size;
    }

    if (settings_.evaluate_frequent_items && !settings_.use_precalculation_optimization) {
      auto& frequent_items_for_col = *column_result.frequent_items_sketch_dictionary;
      auto buffer = dictionary_buffer.GetGenericView();
      std::optional<GenericFrequentItemsSketch> materialized_sketch;
      iceberg::ScopedTimerClock frequent_timer(metrics_per_column_[col_name].frequent_items_);
      if (type == parquet::Type::BYTE_ARRAY) {
        ParquetStringProvider<int64_t> values_provider(reinterpret_cast<const parquet::ByteArray*>(buffer.dictionary),
                                                       buffer.dictionary_length);
        materialized_sketch = frequent_items_for_col.FromDictionary(values_provider);
      } else if (type == parquet::Type::INT64) {
        ParquetInt64Provider<int64_t> values_provider(reinterpret_cast<const int64_t*>(buffer.dictionary),
                                                      buffer.dictionary_length);
        materialized_sketch = frequent_items_for_col.FromDictionary(values_provider);
      } else if (type == parquet::Type::INT32) {
        ParquetInt64Provider<int64_t> values_provider(reinterpret_cast<const int32_t*>(buffer.dictionary),
                                                      buffer.dictionary_length);
        materialized_sketch = frequent_items_for_col.FromDictionary(values_provider);
      }

      if (!materialized_sketch) {
        throw std::runtime_error(std::string(__PRETTY_FUNCTION__) + ": unexpected type " + std::to_string(type));
      }

      column_result.frequent_items_sketch->Merge(*materialized_sketch);

      frequent_items_for_col = GenericFrequentItemsSketch(frequent_items_for_col.Type());
    }

    if (settings_.evaluate_frequent_items && settings_.use_precalculation_optimization) {
      iceberg::ScopedTimerClock frequent_timer(metrics_per_column_[col_name].frequent_items_);
      auto buffer = dictionary_buffer.GetGenericView();

      if (type == parquet::Type::BYTE_ARRAY && settings_.use_string_view_heuristic) {
        FrequentItemsSketch<std::string_view> temp_sketch;
        for (int i = 0; i < buffer.dictionary_length; ++i) {
          auto value = reinterpret_cast<const parquet::ByteArray*>(buffer.dictionary)[i];
          ++frequent_items_updates_;
          temp_sketch.AppendValue(std::string_view(reinterpret_cast<const char*>(value.ptr), value.len),
                                  count_buffer[i]);
        }
        auto serialized_representation = temp_sketch.GetSketch().serialize(0, StringViewSerializer());
        GenericFrequentItemsSketch generic_s(
            FrequentItemsSketch<std::string>(datasketches::frequent_items_sketch<std::string>::deserialize(
                serialized_representation.data(), serialized_representation.size())));

        column_result.frequent_items_sketch->Merge(generic_s);
      } else {
        for (int i = 0; i < buffer.dictionary_length; ++i) {
          if (type == parquet::Type::BYTE_ARRAY) {
            auto value = reinterpret_cast<const parquet::ByteArray*>(buffer.dictionary)[i];
            ++frequent_items_updates_;
            column_result.frequent_items_sketch->AppendWeightedValue(value.ptr, value.len, count_buffer[i]);
          } else if (type == parquet::Type::INT64) {
            auto value = reinterpret_cast<const int64_t*>(buffer.dictionary)[i];
            ++frequent_items_updates_;
            column_result.frequent_items_sketch->AppendWeightedValue(value, count_buffer[i]);
          } else if (type == parquet::Type::INT32) {
            auto value = reinterpret_cast<const int32_t*>(buffer.dictionary)[i];
            ++frequent_items_updates_;
            column_result.frequent_items_sketch->AppendWeightedValue(static_cast<int64_t>(value), count_buffer[i]);
          }
        }
      }
    }

    if (settings_.evaluate_quantiles && settings_.use_precalculation_optimization) {
      iceberg::ScopedTimerClock quantile_timer(metrics_per_column_[col_name].quantile_);
      auto buffer = dictionary_buffer.GetGenericView();

      if (type == parquet::Type::BYTE_ARRAY) {
        datasketches::quantiles_sketch<std::string_view> s;

        for (int i = 0; i < buffer.dictionary_length; ++i) {
          auto value = reinterpret_cast<const parquet::ByteArray*>(buffer.dictionary)[i];
          for (int j = 0; j < count_buffer[i]; ++j) {
            s.update(std::string_view(reinterpret_cast<const char*>(value.ptr), value.len));
          }
        }
        auto serialized_representation = s.serialize(0, StringViewSerializer());

        GenericQuantileSketch generic_s(
            QuantileSketch<std::string>(datasketches::quantiles_sketch<std::string>::deserialize(
                serialized_representation.data(), serialized_representation.size())));

        column_result.quantile_sketch->Merge(generic_s);

      } else {
        for (int i = 0; i < buffer.dictionary_length; ++i) {
          if (type == parquet::Type::INT64) {
            auto value = reinterpret_cast<const int64_t*>(buffer.dictionary)[i];
            for (int j = 0; j < count_buffer[i]; ++j) {
              column_result.quantile_sketch->AppendValue(value);
            }
          } else if (type == parquet::Type::INT32) {
            auto value = reinterpret_cast<const int32_t*>(buffer.dictionary)[i];
            for (int j = 0; j < count_buffer[i]; ++j) {
              column_result.quantile_sketch->AppendValue(static_cast<int64_t>(value));
            }
          }
        }
      }
    }

// TODO(gmusya): quantiles are incorrect because of incorrect comparison operator
#if 0
    if (settings_.evaluate_quantiles) {
      auto& quantiles_for_col = *column_result.quantile_sketch_dictionary;
      auto buffer = dictionary_buffer.GetGenericView();
      if (type == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
        ParquetStringProvider<int64_t> values_provider(
            reinterpret_cast<const parquet::FixedLenByteArray*>(buffer.dictionary), buffer.dictionary_length,
            descr->type_length());
        *column_result.quantile_sketch = quantiles_for_col.FromDictionary(values_provider);
      } else if (type == parquet::Type::BYTE_ARRAY) {
        ParquetStringProvider<int64_t> values_provider(reinterpret_cast<const parquet::ByteArray*>(buffer.dictionary),
                                                       buffer.dictionary_length);
        *column_result.quantile_sketch = quantiles_for_col.FromDictionary(values_provider);
      } else if (type == parquet::Type::INT64) {
        ParquetInt64Provider<int64_t> values_provider(reinterpret_cast<const int64_t*>(buffer.dictionary),
                                                      buffer.dictionary_length);
        *column_result.quantile_sketch = quantiles_for_col.FromDictionary(values_provider);
      } else if (type == parquet::Type::INT32) {
        ParquetInt64Provider<int64_t> values_provider(reinterpret_cast<const int32_t*>(buffer.dictionary),
                                                      buffer.dictionary_length);
        *column_result.quantile_sketch = quantiles_for_col.FromDictionary(values_provider);
      }

      quantiles_for_col = GenericQuantileSketch(quantiles_for_col.Type());
    }
#endif
  } else {
    ++metrics_per_column_[col_name].not_dictionary_encoded_count;

    auto col_reader = rg_reader.Column(col);
    const auto& buffer = buffers.at(col_name);

    std::optional<GenericParquetReader> reader =
        MakeGenericParquetReaderFromType(buffer, col_reader, settings_.batch_size, type);
    iceberg::Ensure(reader.has_value(), "Failed to constructor reader for column " + col_name);

    while (true) {
      std::optional<iceberg::ScopedTimerClock> read_timer(metrics_per_column_[col_name].reading_);
      if (!col_reader->HasNext()) {
        break;
      }
      auto generic_buffer = reader->ReadGeneric();
      read_timer.reset();
      if (settings_.evaluate_distinct) {
        iceberg::ScopedTimerClock counter_timer(metrics_per_column_[col_name].distinct_);
        SketchUpdater updater(counter_for_col);

        updater.AppendValues(generic_buffer.data, generic_buffer.values_size, type);
      }
      if (settings_.evaluate_quantiles) {
        iceberg::ScopedTimerClock quantile_timer(metrics_per_column_[col_name].quantile_);
        auto& quantiles_for_col = *column_result.quantile_sketch;

        if (type == parquet::Type::BYTE_ARRAY && settings_.use_string_view_heuristic) {
          QuantileSketch<std::string_view> temp_sketch;
          for (int i = 0; i < generic_buffer.values_size; ++i) {
            auto value = reinterpret_cast<const parquet::ByteArray*>(generic_buffer.data)[i];
            temp_sketch.AppendValue(std::string_view(reinterpret_cast<const char*>(value.ptr), value.len));
          }
          auto serialized_representation = temp_sketch.GetSketch().serialize(0, StringViewSerializer());
          GenericQuantileSketch generic_s(
              QuantileSketch<std::string>(datasketches::quantiles_sketch<std::string>::deserialize(
                  serialized_representation.data(), serialized_representation.size())));
          column_result.quantile_sketch->Merge(generic_s);
        } else {
          SketchUpdater updater(quantiles_for_col);

          updater.AppendValues(generic_buffer.data, generic_buffer.values_size, type);
        }
      }

      if (settings_.evaluate_frequent_items) {
        iceberg::ScopedTimerClock frequent_timer(metrics_per_column_[col_name].frequent_items_);
        auto& frequent_items_for_col = *column_result.frequent_items_sketch;

        if (type == parquet::Type::BYTE_ARRAY && settings_.use_string_view_heuristic) {
          FrequentItemsSketch<std::string_view> temp_sketch;
          for (int i = 0; i < generic_buffer.values_size; ++i) {
            ++frequent_items_updates_;
            auto value = reinterpret_cast<const parquet::ByteArray*>(generic_buffer.data)[i];
            temp_sketch.AppendValue(std::string_view(reinterpret_cast<const char*>(value.ptr), value.len));
          }
          auto serialized_representation = temp_sketch.GetSketch().serialize(0, StringViewSerializer());
          GenericFrequentItemsSketch generic_s(
              FrequentItemsSketch<std::string>(datasketches::frequent_items_sketch<std::string>::deserialize(
                  serialized_representation.data(), serialized_representation.size())));
          column_result.frequent_items_sketch->Merge(generic_s);
        } else {
          SketchUpdater updater(frequent_items_for_col);
          frequent_items_updates_ += generic_buffer.values_size;
          updater.AppendValues(generic_buffer.data, generic_buffer.values_size, type);
        }
      }
      values_read += generic_buffer.values_size;
    }
  }
}

void Analyzer::Analyze(const std::string& filename, std::optional<int> row_group) {
  std::shared_ptr<arrow::io::RandomAccessFile> input_file;
  if (filename.find("://") != std::string::npos) {
    input_file = iceberg::ValueSafe(settings_.fs->OpenInputFile(filename.substr(filename.find("://") + 3)));
  } else {
    input_file = iceberg::ValueSafe(settings_.fs->OpenInputFile(filename));
  }

  parquet::ReaderProperties props;
  props.enable_buffered_stream();

  auto file_reader = parquet::ParquetFileReader::Open(input_file, props);
  auto metadata = file_reader->metadata();
  auto num_row_groups = metadata->num_row_groups();
  auto num_columns = metadata->num_columns();
  auto num_rows = metadata->num_rows();

  for (int col = 0; col < num_columns; ++col) {
    const auto* column_descriptor = metadata->schema()->Column(col);
    iceberg::Ensure(column_descriptor != nullptr, "No column descriptor for column " + std::to_string(col));

    const auto& name = column_descriptor->name();

    if (settings_.columns_to_ignore.contains(name) ||
        (!settings_.columns_to_process.empty() && !settings_.columns_to_process.contains(name))) {
      continue;
    }

    const auto type = column_descriptor->physical_type();
    if (result_.sketches.contains(name)) {
      iceberg::Ensure(result_.sketches.at(name).type == type, "File " + filename + " contains unexpected type " +
                                                                  std::to_string(type) + " for column '" + name + "'");
      continue;
    }

    auto maybe_buffer = MakeBufferFromType(settings_.batch_size, type);
    iceberg::Ensure(maybe_buffer.has_value(),
                    "Type " + std::to_string(static_cast<int>(type)) + " is not supported for column '" + name + "'");
    buffers.emplace(name, std::move(maybe_buffer.value()));

    auto maybe_dictionary_buffer = MakeDictionaryBufferFromType(settings_.batch_size, type);
    iceberg::Ensure(maybe_dictionary_buffer.has_value(),
                    "Type " + std::to_string(static_cast<int>(type)) + " is not supported for column '" + name + "'");
    dictionary_buffers.emplace(name, std::move(maybe_dictionary_buffer.value()));

    result_.sketches.emplace(name, InitializeSketchesForColumn(*column_descriptor, settings_));
  }

  // std::cerr << filename << ": num_row_groups = " << num_row_groups << std::endl;
  // std::cerr << filename << ": num_columns = " << num_columns << std::endl;
  // std::cerr << filename << ": num_rows = " << num_rows << std::endl;

  std::vector<int> row_groups_to_process;
  if (row_group.has_value()) {
    row_groups_to_process = {row_group.value()};
  } else {
    row_groups_to_process.resize(std::min(num_row_groups, settings_.row_groups_limit.value_or(num_row_groups)));
    std::iota(row_groups_to_process.begin(), row_groups_to_process.end(), 0);
  }

  for (int rg : row_groups_to_process) {
    std::cerr << "Processing row group " << rg << " in file " << filename << std::endl;

    auto rg_reader = file_reader->RowGroup(rg);
    iceberg::Ensure(rg_reader != nullptr, "No RowGroupReader for row group " + std::to_string(rg));

    auto rg_metadata = rg_reader->metadata();
    iceberg::Ensure(rg_reader != nullptr, "No RowGroupMetadata for row group " + std::to_string(rg));

    for (auto col = 0; col < num_columns; ++col) {
      const auto* column_descriptor = metadata->schema()->Column(col);
      const auto& name = column_descriptor->name();
      if (settings_.columns_to_ignore.contains(name) ||
          (!settings_.columns_to_process.empty() && !settings_.columns_to_process.contains(name))) {
        continue;
      }
      AnalyzeColumn(*rg_metadata, col, *rg_reader.get());
    }
  }
}

void Analyzer::PrintTimings() {
  if (settings_.print_timings) {
    auto serialize_nanos = [](iceberg::DurationClock point) -> std::string {
      auto millis_total = std::chrono::duration_cast<std::chrono::milliseconds>(point);
      std::stringstream ss;
      auto seconds = millis_total.count() / 1000;
      auto millis = millis_total.count() % 1000;
      ss << seconds << "." << std::setw(3) << std::setfill('0') << millis << "s";

      return ss.str();
    };

    Metrics total_metrics{};

    for (const auto& [name, metrics] : metrics_per_column_) {
      total_metrics.counting_ += metrics.counting_;
      total_metrics.distinct_ += metrics.distinct_;
      total_metrics.frequent_items_ += metrics.frequent_items_;
      total_metrics.quantile_ += metrics.quantile_;
      total_metrics.reading_ += metrics.reading_;
      total_metrics.dictionary_encoded_count_ += metrics.dictionary_encoded_count_;
      total_metrics.not_dictionary_encoded_count += metrics.not_dictionary_encoded_count;
    }

    metrics_per_column_["::total_metrics"] = total_metrics;

    std::vector<std::pair<std::string, int64_t>> results;
    for (const auto& [name, metrics] : metrics_per_column_) {
      auto total =
          (metrics.counting_ + metrics.distinct_ + metrics.frequent_items_ + metrics.quantile_ + metrics.reading_)
              .count();

      results.emplace_back(name, total);
    }
    std::sort(results.begin(), results.end(),
              [&](const auto& lhs, const auto& rhs) { return lhs.second < rhs.second; });

    std::cerr << std::string(80, '-') << std::endl;
    for (const auto& [name, _] : results) {
      const auto& metrics = metrics_per_column_.at(name);
      auto total =
          metrics.counting_ + metrics.distinct_ + metrics.frequent_items_ + metrics.quantile_ + metrics.reading_;

      std::cerr << "Column name: " << name << std::endl;
      std::cerr << "Dicitonary encoded: " << metrics.dictionary_encoded_count_ << "/"
                << metrics.dictionary_encoded_count_ + metrics.not_dictionary_encoded_count << std::endl;
      if (metrics.counting_.count() != 0) {
        std::cerr << "counting: " << serialize_nanos(metrics.counting_) << std::endl;
      }
      if (metrics.distinct_.count() != 0) {
        std::cerr << "distinct: " << serialize_nanos(metrics.distinct_) << std::endl;
      }
      if (metrics.quantile_.count() != 0) {
        std::cerr << "quantile: " << serialize_nanos(metrics.quantile_) << std::endl;
      }
      if (metrics.frequent_items_.count() != 0) {
        std::cerr << "frequent_items: " << serialize_nanos(metrics.frequent_items_) << std::endl;
      }
      if (metrics.reading_.count() != 0) {
        std::cerr << "reading: " << serialize_nanos(metrics.reading_) << std::endl;
      }
      std::cerr << "total: " << serialize_nanos(total) << std::endl;
      std::cerr << std::string(80, '-') << std::endl;
    }
  }

  std::cerr << "frequent_items_updates: " << frequent_items_updates_ << std::endl;
}

}  // namespace stats
