#pragma once

#include <GenericDatum.hh>
#include <array>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "avro/Config.hh"
#include "avro/DataFile.hh"
#include "avro/Encoder.hh"
#include "avro/Specific.hh"
#include "avro/Stream.hh"
#include "avro/ValidSchema.hh"
#include "avro/buffer/Buffer.hh"

namespace iceberg {

const int SyncSize = 16;
/**
 * The sync value.
 */
typedef std::array<uint8_t, SyncSize> DataFileSync;

class MyMemoryInputStream : public avro::InputStream {
  const uint8_t* const data_;
  const size_t size_;
  size_t curLen_;

 public:
  MyMemoryInputStream(const uint8_t* data, size_t len) : data_(data), size_(len), curLen_(0) {}

  bool next(const uint8_t** data, size_t* len) final {
    if (curLen_ == size_) {
      return false;
    }
    *data = &data_[curLen_];
    *len = size_ - curLen_;
    curLen_ = size_;
    return true;
  }

  void backup(size_t len) final { curLen_ -= len; }

  void skip(size_t len) final {
    if (len > (size_ - curLen_)) {
      len = size_ - curLen_;
    }
    curLen_ += len;
  }

  size_t current_offset() const { return curLen_; }

  void seek(size_t offset) { curLen_ = offset; }

  size_t byteCount() const final { return curLen_; }
};

inline std::unique_ptr<MyMemoryInputStream> myMemoryInputStream(const uint8_t* data, size_t len) {
  return std::unique_ptr<MyMemoryInputStream>(new MyMemoryInputStream(data, len));
}

/**
 * The type independent portion of reader.
 */
class XReader {
 public:
  typedef std::map<std::string, std::vector<uint8_t>> Metadata;

 private:
  const std::string filename_;
  std::unique_ptr<avro::InputStream> stream_;
  const avro::DecoderPtr decoder_;
  int64_t objectCount_;
  bool eof_;
  avro::Codec codec_;
  int64_t blockStart_{};
  int64_t blockEnd_{};

  avro::ValidSchema readerSchema_;
  avro::ValidSchema dataSchema_;
  avro::ValidSchema oneFieldSchema_;
  avro::DecoderPtr dataDecoder_;
  avro::DecoderPtr oneFieldDecoder_;
  std::unique_ptr<MyMemoryInputStream> dataStream_;

  Metadata metadata_;
  DataFileSync sync_{};

  // for compressed buffer
  std::vector<char> compressed_;
  std::string uncompressed;
  void readHeader();

  void readDataBlock();
#if 0
  void doSeek(int64_t position);
#endif
 public:
  /**
   * Returns the current decoder for this reader.
   */
  avro::Decoder& decoder() { return *dataDecoder_; }
  avro::Decoder& one_field_decoder() { return *oneFieldDecoder_; }

  /**
   * Returns true if and only if there is more to read.
   */
  bool hasMore();

  /**
   * Decrements the number of objects yet to read.
   */
  void decr() { --objectCount_; }

  /**
   * Constructs the reader for the given file and the reader is
   * expected to use the schema that is used with data.
   * This function should be called exactly once after constructing
   * the DataFileReaderBase object.
   */
  explicit XReader(const char* filename);

  explicit XReader(std::unique_ptr<avro::InputStream> inputStream);

  XReader(const XReader&) = delete;
  XReader& operator=(const XReader&) = delete;

  /**
   * Initializes the reader so that the reader and writer schemas
   * are the same.
   */
  void init();

  /**
   * Initializes the reader to read objects according to the given
   * schema. This gives an opportunity for the reader to see the schema
   * in the data file before deciding the right schema to use for reading.
   * This must be called exactly once after constructing the
   * DataFileReaderBase object.
   */
  void init(const avro::ValidSchema& readerSchema);

  /**
   * Returns the schema for this object.
   */
  const avro::ValidSchema& readerSchema() { return readerSchema_; }

  /**
   * Returns the schema stored with the data file.
   */
  const avro::ValidSchema& dataSchema() { return dataSchema_; }

  const avro::ValidSchema& oneFieldSchema() { return oneFieldSchema_; }

  std::unique_ptr<MyMemoryInputStream>& dataStream() { return dataStream_; }

  /**
   * Closes the reader. No further operation is possible on this reader.
   */
  void close();

#if 0
  /**
   * Move to a specific, known synchronization point, for example one returned
   * from tell() after sync().
   */
  void seek(int64_t position);

  /**
   * Move to the next synchronization point after a position. To process a
   * range of file entries, call this with the starting position, then check
   * pastSync() with the end point before each use of decoder().
   */
  void sync(int64_t position);

  /**
   * Return true if past the next synchronization point after a position.
   */
  bool pastSync(int64_t position);

  /**
   * Return the last synchronization point before our current position.
   */
  int64_t previousSync() const;
#endif

  /**
   * Return file metadata.
   */
  const auto& metadata() const { return metadata_; }
};

/**
 * Reads the contents of data file one after another.
 */
template <typename T>
class YReader {
  std::unique_ptr<XReader> base_;

 public:
  /**
   * Constructs the reader for the given file and the reader is
   * expected to use the given schema.
   */
  YReader(const char* filename, const avro::ValidSchema& readerSchema) : base_(new XReader(filename)) {
    base_->init(readerSchema);
  }

  YReader(std::unique_ptr<avro::InputStream> inputStream, const avro::ValidSchema& readerSchema)
      : base_(new XReader(std::move(inputStream))) {
    base_->init(readerSchema);
  }

  /**
   * Constructs the reader for the given file and the reader is
   * expected to use the schema that is used with data.
   */
  explicit YReader(const char* filename) : base_(new XReader(filename)) { base_->init(); }

  explicit YReader(std::unique_ptr<avro::InputStream> inputStream) : base_(new XReader(std::move(inputStream))) {
    base_->init();
  }

  /**
   * Constructs a reader using the reader base. This form of constructor
   * allows the user to examine the schema of a given file and then
   * decide to use the right type of data to be deserialize. Without this
   * the user must know the type of data for the template _before_
   * he knows the schema within the file.
   * The schema present in the data file will be used for reading
   * from this reader.
   */
  explicit YReader(std::unique_ptr<XReader> base) : base_(std::move(base)) { base_->init(); }

  /**
   * Constructs a reader using the reader base. This form of constructor
   * allows the user to examine the schema of a given file and then
   * decide to use the right type of data to be deserialize. Without this
   * the user must know the type of data for the template _before_
   * he knows the schema within the file.
   * The argument readerSchema will be used for reading
   * from this reader.
   */
  YReader(std::unique_ptr<XReader> base, const avro::ValidSchema& readerSchema) : base_(std::move(base)) {
    base_->init(readerSchema);
  }

  YReader(const YReader&) = delete;
  YReader& operator=(const YReader&) = delete;

  /**
   * Reads the next entry from the data file.
   * \return true if an object has been successfully read into \p datum and
   * false if there are no more entries in the file.
   */
  bool read(T& datum) {
    while (true) {
      if (base_->hasMore()) {
        base_->decr();

        auto& data_stream = *base_->dataStream();
        size_t current_offset = data_stream.current_offset();

        avro::GenericDatum one_field(base_->oneFieldSchema());
        avro::decode(base_->one_field_decoder(), one_field);
        int32_t status = one_field.value<avro::GenericRecord>().field("status").value<int32_t>();
        if (status == 2) {
          continue;
        }

        data_stream.seek(current_offset);
        avro::decode(base_->decoder(), datum);
        return true;
      }
      return false;
    }
  }

  /**
   * Returns the schema for this object.
   */
  const avro::ValidSchema& readerSchema() { return base_->readerSchema(); }

  /**
   * Returns the schema stored with the data file.
   */
  const avro::ValidSchema& dataSchema() { return base_->dataSchema(); }

  /**
   * Closes the reader. No further operation is possible on this reader.
   */
  void close() { return base_->close(); }

#if 0
  /**
   * Move to a specific, known synchronization point, for example one returned
   * from previousSync().
   */
  void seek(int64_t position) { base_->seek(position); }

  /**
   * Move to the next synchronization point after a position. To process a
   * range of file entries, call this with the starting position, then check
   * pastSync() with the end point before each call to read().
   */
  void sync(int64_t position) { base_->sync(position); }

  /**
   * Return true if past the next synchronization point after a position.
   */
  bool pastSync(int64_t position) { return base_->pastSync(position); }

  /**
   * Return the last synchronization point before our current position.
   */
  int64_t previousSync() { return base_->previousSync(); }
#endif

  /**
   * Return file metadata.
   */
  const auto& metadata() const { return base_->metadata(); }
};

}  // namespace iceberg
