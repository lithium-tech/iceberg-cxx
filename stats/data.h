#pragma once

#include <memory>

#include "parquet/column_reader.h"
#include "parquet/file_reader.h"
#include "parquet/types.h"

using ParquetType = parquet::Type::type;

template <ParquetType parquet_type>
struct Traits {};

template <>
struct Traits<ParquetType::INT32> {
  using PhysicalType = int32_t;
  using ReaderType = parquet::Int32Reader;
};

template <>
struct Traits<ParquetType::INT64> {
  using PhysicalType = int64_t;
  using ReaderType = parquet::Int64Reader;
};

template <>
struct Traits<ParquetType::FIXED_LEN_BYTE_ARRAY> {
  using PhysicalType = parquet::FixedLenByteArray;
  using ReaderType = parquet::FixedLenByteArrayReader;
};

template <>
struct Traits<ParquetType::BYTE_ARRAY> {
  using PhysicalType = parquet::ByteArray;
  using ReaderType = parquet::ByteArrayReader;
};

using Int16Data = int16_t[];
using Int32Data = int32_t[];
using Int64Data = int64_t[];
using FLBAData = parquet::FixedLenByteArray[];
using ByteArrayData = parquet::ByteArray[];

struct ParquetViewCommon {
  uint64_t values_size;
  uint64_t levels_size;
  const int16_t* def_levels;
  const int16_t* rep_levels;
};

template <ParquetType parquet_type>
struct ParquetBufferView : public ParquetViewCommon {
  const Traits<parquet_type>::PhysicalType* data;
};

struct GenericParquetBufferView : public ParquetViewCommon {
  const void* data;
  ParquetType parquet_type;
};

template <ParquetType parquet_type>
struct ParquetBuffer {
  using Type = Traits<parquet_type>::PhysicalType;

  int64_t values_size;
  int64_t levels_size;
  std::unique_ptr<Int16Data> def_levels;
  std::unique_ptr<Int16Data> rep_levels;
  std::unique_ptr<Type[]> data;

  explicit ParquetBuffer(int64_t batch_size) {
    def_levels = std::make_unique<Int16Data>(batch_size);
    rep_levels = std::make_unique<Int16Data>(batch_size);
    data = std::make_unique<Type[]>(batch_size);
  }

  ParquetBufferView<parquet_type> MakeView() {
    ParquetBufferView<parquet_type> view;
    view.values_size = values_size;
    view.levels_size = levels_size;
    view.def_levels = def_levels.get();
    view.rep_levels = rep_levels.get();
    view.data = data.get();
    return view;
  }
};

template <ParquetType parquet_type>
GenericParquetBufferView MakeGenericView(const ParquetBufferView<parquet_type>& view) {
  GenericParquetBufferView result;
  result.data = view.data;
  result.def_levels = view.def_levels;
  result.rep_levels = view.rep_levels;
  result.values_size = view.values_size;
  result.levels_size = view.levels_size;
  result.parquet_type = parquet_type;
  return result;
}

class GenericParquetBuffer {
 public:
  template <typename TypedBuffer>
  explicit GenericParquetBuffer(std::shared_ptr<TypedBuffer> buffer) {
    buffer_.emplace<std::shared_ptr<TypedBuffer>>(buffer);
  }

  template <ParquetType parquet_type>
  auto Get() const {
    using BufferType = ParquetBuffer<parquet_type>;

    return std::get<std::shared_ptr<BufferType>>(buffer_);
  }

  GenericParquetBufferView GetGenericView() const {
    return std::visit(
        [](auto&& arg) {
          auto view = arg->MakeView();
          return MakeGenericView(view);
        },
        buffer_);
  }

 private:
  std::variant<std::shared_ptr<ParquetBuffer<ParquetType::INT32>>, std::shared_ptr<ParquetBuffer<ParquetType::INT64>>,
               std::shared_ptr<ParquetBuffer<ParquetType::BYTE_ARRAY>>,
               std::shared_ptr<ParquetBuffer<ParquetType::FIXED_LEN_BYTE_ARRAY>>>
      buffer_;
};

using Int32ParquetBuffer = ParquetBuffer<ParquetType::INT32>;
using Int64ParquetBuffer = ParquetBuffer<ParquetType::INT64>;
using FLBAParquetBuffer = ParquetBuffer<ParquetType::FIXED_LEN_BYTE_ARRAY>;
using ByteArrayParquetBuffer = ParquetBuffer<ParquetType::BYTE_ARRAY>;

template <ParquetType parquet_type>
class ParquetReader {
  using PhysicalType = Traits<parquet_type>::PhysicalType;
  using ReaderType = Traits<parquet_type>::ReaderType;

 public:
  explicit ParquetReader(std::shared_ptr<ReaderType> typed_reader, std::shared_ptr<ParquetBuffer<parquet_type>> buffer,
                         int64_t batch_size)
      : typed_reader_(typed_reader), buffer_(buffer), batch_size_(batch_size) {}

  ParquetBufferView<parquet_type> Read() {
    buffer_->levels_size = typed_reader_->ReadBatch(batch_size_, buffer_->def_levels.get(), buffer_->rep_levels.get(),
                                                    buffer_->data.get(), &(buffer_->values_size));
    return buffer_->MakeView();
  }

 private:
  std::shared_ptr<ReaderType> typed_reader_;
  std::shared_ptr<ParquetBuffer<parquet_type>> buffer_;
  int64_t batch_size_;
};

using Int32ParquetReader = ParquetReader<ParquetType::INT32>;
using Int64ParquetReader = ParquetReader<ParquetType::INT64>;
using FLBAParquetReader = ParquetReader<ParquetType::FIXED_LEN_BYTE_ARRAY>;
using ByteArrayParquetReader = ParquetReader<ParquetType::BYTE_ARRAY>;

class GenericParquetReader {
 public:
  template <typename ParquetReader>
  explicit GenericParquetReader(std::shared_ptr<ParquetReader> reader) {
    reader_.emplace<std::shared_ptr<ParquetReader>>(reader);
  }

// unused currently, so comment out
#if 0
  template <ParquetType parquet_type>
  ParquetBufferView<parquet_type> Read(int64_t batch_size) {
    using ReaderType = ParquetReader<parquet_type>;
    using ReaderTypePtr = std::shared_ptr<ReaderType>;

    return std::get<ReaderTypePtr>(reader_)->Read();
  }
#endif

  GenericParquetBufferView ReadGeneric() {
    return std::visit(
        [](auto&& arg) {
          auto res = arg->Read();
          return MakeGenericView(res);
        },
        reader_);
  }

  std::variant<std::shared_ptr<Int32ParquetReader>, std::shared_ptr<Int64ParquetReader>,
               std::shared_ptr<FLBAParquetReader>, std::shared_ptr<ByteArrayParquetReader>>
      reader_;
};
