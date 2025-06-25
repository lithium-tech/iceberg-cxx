/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "iceberg/avro_manifest_entry_reader.h"

#include <Decoder.hh>
#include <random>
#include <sstream>

#include "avro/Compiler.hh"
#include "avro/DataFile.hh"
#include "avro/Exception.hh"

#ifdef SNAPPY_CODEC_AVAILABLE
#include <snappy.h>
#endif

#ifdef ZSTD_CODEC_AVAILABLE
#include <zstd.h>
#endif

#include <zlib.h>

namespace iceberg {
using std::copy;
using std::istringstream;
using std::ostringstream;
using std::string;
using std::unique_ptr;
using std::vector;

using std::array;

namespace {
const string AVRO_SCHEMA_KEY("avro.schema");
const string AVRO_CODEC_KEY("avro.codec");
const string AVRO_NULL_CODEC("null");
const string AVRO_DEFLATE_CODEC("deflate");

#ifdef SNAPPY_CODEC_AVAILABLE
const string AVRO_SNAPPY_CODEC = "snappy";
#endif

#ifdef ZSTD_CODEC_AVAILABLE
const string AVRO_ZSTD_CODEC = "zstd";
#endif

const size_t minSyncInterval = 32;
const size_t maxSyncInterval = 1u << 30;

// Recommended by https://www.zlib.net/zlib_how.html
const size_t zlibBufGrowSize = 128 * 1024;

}  // namespace

typedef array<uint8_t, 4> Magic;
static Magic magic = {{'O', 'b', 'j', '\x01'}};

XReader::XReader(const char* filename)
    : filename_(filename),
      stream_(avro::fileSeekableInputStream(filename)),
      decoder_(avro::binaryDecoder()),
      objectCount_(0),
      eof_(false),
      codec_(avro::NULL_CODEC),
      blockStart_(-1),
      blockEnd_(-1) {
  readHeader();
}

XReader::XReader(std::unique_ptr<avro::InputStream> inputStream)
    : stream_(std::move(inputStream)),
      decoder_(avro::binaryDecoder()),
      objectCount_(0),
      eof_(false),
      codec_(avro::NULL_CODEC) {
  readHeader();
}

constexpr std::string_view kOnlyStatusSchema = R"EOF(
{
    "type": "record",
    "name": "manifest_entry",
    "fields": [
        {
            "name": "status",
            "type": "int",
            "default": 2
        }
    ]
})EOF";

avro::ValidSchema OnlyStatusSchema() {
  avro::ValidSchema result;
  std::stringstream in(kOnlyStatusSchema.data());
  avro::compileJsonSchema(in, result);
  return result;
};

void XReader::init() {
  readerSchema_ = dataSchema_;
  oneFieldSchema_ = OnlyStatusSchema();
  dataDecoder_ = avro::binaryDecoder();
  oneFieldDecoder_ = (oneFieldSchema_.toJson(true) != dataSchema_.toJson(true))
                         ? resolvingDecoder(dataSchema_, oneFieldSchema_, avro::binaryDecoder())
                         : avro::binaryDecoder();
  readDataBlock();
}

void XReader::init(const avro::ValidSchema& readerSchema) {
  readerSchema_ = readerSchema;
  oneFieldSchema_ = OnlyStatusSchema();
  dataDecoder_ = (readerSchema_.toJson(true) != dataSchema_.toJson(true))
                     ? resolvingDecoder(dataSchema_, readerSchema_, avro::binaryDecoder())
                     : avro::binaryDecoder();
  oneFieldDecoder_ = (oneFieldSchema_.toJson(true) != dataSchema_.toJson(true))
                         ? resolvingDecoder(dataSchema_, oneFieldSchema_, avro::binaryDecoder())
                         : avro::binaryDecoder();
  readDataBlock();
}

static void drain(avro::InputStream& in) {
  const uint8_t* p = nullptr;
  size_t n = 0;
  while (in.next(&p, &n)) {
  }
}

char hex(unsigned int x) { return static_cast<char>(x + (x < 10 ? '0' : ('a' - 10))); }

std::ostream& operator<<(std::ostream& os, const DataFileSync& s) {
  for (uint8_t i : s) {
    os << hex(i / 16) << hex(i % 16) << ' ';
  }
  os << std::endl;
  return os;
}

bool XReader::hasMore() {
  for (;;) {
    if (eof_) {
      return false;
    } else if (objectCount_ != 0) {
      return true;
    }

    dataDecoder_->init(*dataStream_);
    oneFieldDecoder_->init(*dataStream_);
    drain(*dataStream_);
    DataFileSync s;
    decoder_->init(*stream_);
    avro::decode(*decoder_, s);
    if (s != sync_) {
      throw avro::Exception("Sync mismatch");
    }
    readDataBlock();
  }
}

class BoundedInputStream : public avro::InputStream {
  InputStream& in_;
  size_t limit_;

  bool next(const uint8_t** data, size_t* len) final {
    if (limit_ != 0 && in_.next(data, len)) {
      if (*len > limit_) {
        in_.backup(*len - limit_);
        *len = limit_;
      }
      limit_ -= *len;
      return true;
    }
    return false;
  }

  void backup(size_t len) final {
    in_.backup(len);
    limit_ += len;
  }

  void skip(size_t len) final {
    if (len > limit_) {
      len = limit_;
    }
    in_.skip(len);
    limit_ -= len;
  }

  size_t byteCount() const final { return in_.byteCount(); }

 public:
  BoundedInputStream(InputStream& in, size_t limit) : in_(in), limit_(limit) {}
};

unique_ptr<avro::InputStream> boundedInputStream(avro::InputStream& in, size_t limit) {
  return unique_ptr<avro::InputStream>(new BoundedInputStream(in, limit));
}

void XReader::readDataBlock() {
  decoder_->init(*stream_);
  blockStart_ = stream_->byteCount();
  const uint8_t* p = nullptr;
  size_t n = 0;
  if (!stream_->next(&p, &n)) {
    eof_ = true;
    return;
  }
  stream_->backup(n);
  avro::decode(*decoder_, objectCount_);
  int64_t byteCount;
  avro::decode(*decoder_, byteCount);
  decoder_->init(*stream_);
  blockEnd_ = stream_->byteCount() + byteCount;

  unique_ptr<avro::InputStream> st = boundedInputStream(*stream_, static_cast<size_t>(byteCount));
  if (codec_ == avro::NULL_CODEC) {
    uncompressed.clear();
    const uint8_t* data;
    size_t len;
    while (st->next(&data, &len)) {
      uncompressed.insert(uncompressed.end(), data, data + len);
    }

    std::unique_ptr<MyMemoryInputStream> in =
        myMemoryInputStream(reinterpret_cast<const uint8_t*>(uncompressed.c_str()), uncompressed.size());

    dataDecoder_->init(*in);
    oneFieldDecoder_->init(*in);
    dataStream_ = std::move(in);
#ifdef SNAPPY_CODEC_AVAILABLE
  } else if (codec_ == avro::SNAPPY_CODEC) {
    uint32_t checksum = 0;
    compressed_.clear();
    uncompressed.clear();
    const uint8_t* data;
    size_t len;
    while (st->next(&data, &len)) {
      compressed_.insert(compressed_.end(), data, data + len);
    }
    len = compressed_.size();
    if (len < 4)
      throw avro::Exception("Cannot read compressed data, expected at least 4 bytes, got " + std::to_string(len));

    int b1 = compressed_[len - 4] & 0xFF;
    int b2 = compressed_[len - 3] & 0xFF;
    int b3 = compressed_[len - 2] & 0xFF;
    int b4 = compressed_[len - 1] & 0xFF;

    checksum = (b1 << 24) + (b2 << 16) + (b3 << 8) + (b4);
    if (!snappy::Uncompress(reinterpret_cast<const char*>(compressed_.data()), len - 4, &uncompressed)) {
      throw avro::Exception("Snappy Compression reported an error when decompressing");
    }
    auto c = crc32(0, reinterpret_cast<const Bytef*>(uncompressed.c_str()), static_cast<uInt>(uncompressed.size()));
    if (checksum != c) {
      throw avro::Exception("Checksum did not match for Snappy compression: Expected: {}, computed: {}", checksum, c);
    }

    std::unique_ptr<MyMemoryInputStream> in =
        myMemoryInputStream(reinterpret_cast<const uint8_t*>(uncompressed.c_str()), uncompressed.size());

    dataDecoder_->init(*in);
    oneFieldDecoder_->init(*in);
    dataStream_ = std::move(in);
#endif
#ifdef ZSTD_CODEC_AVAILABLE
  } else if (codec_ == ZSTD_CODEC) {
    compressed_.clear();
    const uint8_t* data;
    size_t len;
    while (st->next(&data, &len)) {
      compressed_.insert(compressed_.end(), data, data + len);
    }

    // Get the decompressed size
    size_t decompressed_size =
        ZSTD_getFrameContentSize(reinterpret_cast<const char*>(compressed_.data()), compressed_.size());
    if (decompressed_size == ZSTD_CONTENTSIZE_ERROR) {
      throw Exception("ZSTD: Not a valid compressed frame");
    } else if (decompressed_size == ZSTD_CONTENTSIZE_UNKNOWN) {
      throw Exception("ZSTD: Unable to determine decompressed size");
    }

    // Decompress the data
    uncompressed.clear();
    uncompressed.resize(decompressed_size);
    size_t result = ZSTD_decompress(uncompressed.data(), decompressed_size,
                                    reinterpret_cast<const char*>(compressed_.data()), compressed_.size());

    if (ZSTD_isError(result)) {
      throw Exception("ZSTD decompression error: {}", ZSTD_getErrorName(result));
    }
    if (result != decompressed_size) {
      throw Exception("ZSTD: Decompressed size mismatch: expected {}, got {}", decompressed_size, result);
    }

    std::unique_ptr<MyMemoryInputStream> in =
        myMemoryInputStream(reinterpret_cast<const uint8_t*>(uncompressed.c_str()), uncompressed.size());

    dataDecoder_->init(*in);
    oneFieldDecoder_->init(*in);
    dataStream_ = std::move(in);
#endif
  } else {
    compressed_.clear();
    uncompressed.clear();

    {
      z_stream zs;
      zs.zalloc = Z_NULL;
      zs.zfree = Z_NULL;
      zs.opaque = Z_NULL;
      zs.avail_in = 0;
      zs.next_in = Z_NULL;

      int ret = inflateInit2(&zs, /*windowBits=*/-15);
      if (ret != Z_OK) {
        throw avro::Exception("Failed to initialize inflate, error: {}", ret);
      }

      const uint8_t* data;
      size_t len;
      while (ret != Z_STREAM_END && st->next(&data, &len)) {
        zs.avail_in = static_cast<uInt>(len);
        zs.next_in = const_cast<Bytef*>(data);
        do {
          if (zs.total_out == uncompressed.size()) {
            uncompressed.resize(uncompressed.size() + zlibBufGrowSize);
          }
          zs.avail_out = static_cast<uInt>(uncompressed.size() - zs.total_out);
          zs.next_out = reinterpret_cast<Bytef*>(uncompressed.data() + zs.total_out);
          ret = inflate(&zs, Z_NO_FLUSH);
          if (ret == Z_STREAM_END) {
            break;
          }
          if (ret != Z_OK) {
            throw avro::Exception("Failed to inflate, error: {}", ret);
          }
        } while (zs.avail_out == 0);
      }

      uncompressed.resize(zs.total_out);
      (void)inflateEnd(&zs);
    }

    std::unique_ptr<MyMemoryInputStream> in =
        myMemoryInputStream(reinterpret_cast<const uint8_t*>(uncompressed.c_str()), uncompressed.size());

    dataDecoder_->init(*in);
    oneFieldDecoder_->init(*in);
    dataStream_ = std::move(in);
  }
}

void XReader::close() {
  stream_.reset();
  eof_ = true;
  objectCount_ = 0;
  blockStart_ = 0;
  blockEnd_ = 0;
}

static string toString(const vector<uint8_t>& v) {
  string result;
  result.resize(v.size());
  copy(v.begin(), v.end(), result.begin());
  return result;
}

static avro::ValidSchema makeSchema(const vector<uint8_t>& v) {
  istringstream iss(toString(v));
  avro::ValidSchema vs;
  compileJsonSchema(iss, vs);
  return vs;
}

void XReader::readHeader() {
  decoder_->init(*stream_);
  Magic m;
  avro::decode(*decoder_, m);
  if (magic != m) {
    throw avro::Exception("Invalid data file. Magic does not match: " + filename_);
  }
  avro::decode(*decoder_, metadata_);
  Metadata::const_iterator it = metadata_.find(AVRO_SCHEMA_KEY);
  if (it == metadata_.end()) {
    throw avro::Exception("No schema in metadata");
  }

  dataSchema_ = makeSchema(it->second);
  if (!readerSchema_.root()) {
    readerSchema_ = dataSchema();
  }

  it = metadata_.find(AVRO_CODEC_KEY);
  if (it != metadata_.end() && toString(it->second) == AVRO_DEFLATE_CODEC) {
    codec_ = avro::DEFLATE_CODEC;
#ifdef SNAPPY_CODEC_AVAILABLE
  } else if (it != metadata_.end() && toString(it->second) == AVRO_SNAPPY_CODEC) {
    codec_ = avro::SNAPPY_CODEC;
#endif
#ifdef ZSTD_CODEC_AVAILABLE
  } else if (it != metadata_.end() && toString(it->second) == AVRO_ZSTD_CODEC) {
    codec_ = ZSTD_CODEC;
#endif
  } else {
    codec_ = avro::NULL_CODEC;
    if (it != metadata_.end() && toString(it->second) != AVRO_NULL_CODEC) {
      throw avro::Exception("Unknown codec in data file: " + toString(it->second));
    }
  }

  avro::decode(*decoder_, sync_);
  decoder_->init(*stream_);
  blockStart_ = stream_->byteCount();
}

#if 0
void XReader::doSeek(int64_t position) {
  if (auto* ss = dynamic_cast<avro::SeekableInputStream*>(stream_.get())) {
    if (!eof_) {
      dataDecoder_->init(*dataStream_);
      drain(*dataStream_);
    }
    decoder_->init(*stream_);
    ss->seek(position);
    eof_ = false;
  } else {
    throw avro::Exception("seek not supported on non-SeekableInputStream");
  }
}
#endif

#if 0
void XReader::seek(int64_t position) {
  doSeek(position);
  readDataBlock();
}

void XReader::sync(int64_t position) {
  doSeek(position);
  DataFileSync sync_buffer;
  const uint8_t* p = nullptr;
  size_t n = 0;
  size_t i = 0;
  while (i < SyncSize) {
    if (n == 0 && !stream_->next(&p, &n)) {
      eof_ = true;
      return;
    }
    size_t len = std::min(SyncSize - i, n);
    memcpy(&sync_buffer[i], p, len);
    p += len;
    n -= len;
    i += len;
  }
  for (;;) {
    size_t j = 0;
    for (; j < SyncSize; ++j) {
      if (sync_[j] != sync_buffer[(i + j) % SyncSize]) {
        break;
      }
    }
    if (j == SyncSize) {
      // Found the sync marker!
      break;
    }
    if (n == 0 && !stream_->next(&p, &n)) {
      eof_ = true;
      return;
    }
    sync_buffer[i++ % SyncSize] = *p++;
    --n;
  }
  stream_->backup(n);
  readDataBlock();
}

bool XReader::pastSync(int64_t position) { return !hasMore() || blockStart_ >= position + SyncSize; }

int64_t XReader::previousSync() const { return blockStart_; }
#endif

}  // namespace iceberg
