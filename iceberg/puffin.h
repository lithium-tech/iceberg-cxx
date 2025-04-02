#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "arrow/result.h"
#include "arrow/status.h"

namespace iceberg {

constexpr std::string_view kPuffinMagicBytes = "\x50\x46\x41\x31";
static_assert(kPuffinMagicBytes.size() == 4);

// PuffinFile: <Magic> <Blob>* <Footer>
class PuffinFile {
 public:
  static arrow::Result<PuffinFile> Make(std::string data) {
    if (data.size() < 4) {
      return arrow::Status::ExecutionError("PuffinFile is incorrect: file is too small (", data.size(), ")");
    }
    if (data.substr(0, 4) != kPuffinMagicBytes) {
      return arrow::Status::ExecutionError("PuffinFile is incorrect: magic bytes are incorrect (expected ",
                                           kPuffinMagicBytes, ", found ", data.substr(0, 4), ")");
    }
    ARROW_ASSIGN_OR_RAISE(auto footer, MakeFooter(data));
    return PuffinFile(std::move(data), std::move(footer));
  }

  const std::string& GetPayload() { return data_; }

  // Footer: <Magic> <FooterPayload> <FooterPayloadSize> <Flags> <Magic>
  class Footer {
   public:
    struct BlobMetadata {
      std::string type;
      std::vector<int32_t> fields;
      int64_t snapshot_id;
      int64_t sequence_number;
      int64_t offset;
      int64_t length;
      std::optional<std::string> compression_codec;
      std::map<std::string, std::string> properties;

      bool operator==(const BlobMetadata& other) const = default;
    };

    struct DeserializedFooter {
      std::vector<BlobMetadata> blobs;
      std::map<std::string, std::string> properties;
    };

    explicit Footer(std::string payload) : payload_(std::move(payload)) {}

    const std::string& GetPayload() const { return payload_; }

    size_t GetBlobsCount() const {
      auto footer = GetDeserializedFooter();
      return footer.blobs.size();
    }

    BlobMetadata GetBlobMetadata(size_t blob_number) const {
      auto footer = GetDeserializedFooter();
      if (blob_number >= footer.blobs.size()) {
        throw arrow::Status::ExecutionError(std::string(__PRETTY_FUNCTION__), ": requested blob number (", blob_number,
                                            ") does not exist (total blob count is ", footer.blobs.size(), ")");
      }
      return footer.blobs.at(blob_number);
    }

    DeserializedFooter GetDeserializedFooter() const;

   private:
    std::string payload_;
    mutable std::optional<DeserializedFooter> deserialized_footer_cache_;
  };

  const Footer& GetFooter() { return footer_; }

  std::string GetBlob(size_t blob_number) const {
    auto blob_meta = footer_.GetBlobMetadata(blob_number);
    int64_t offset = blob_meta.offset;
    int64_t length = blob_meta.length;

    if (offset < 0 || length < 0 || static_cast<size_t>(offset + length) >= data_.size()) {
      throw arrow::Status::ExecutionError(std::string(__PRETTY_FUNCTION__),
                                          ": unexpected blob request (offset = ", offset, ", length = ", length,
                                          ") for file with size ", data_.size());
    }

    return data_.substr(offset, length);
  }

 private:
  explicit PuffinFile(std::string data, Footer footer) : data_(std::move(data)), footer_(std::move(footer)) {}

  static arrow::Result<Footer> MakeFooter(const std::string& data);

 private:
  std::string data_;

  Footer footer_;
};

class PuffinFileBuilder {
 public:
  PuffinFile Build() &&;

  PuffinFileBuilder& SetSequenceNumber(int64_t sequence_number) {
    sequence_number_ = sequence_number;
    return *this;
  }

  PuffinFileBuilder& SetSnapshotId(int64_t snapshot_id) {
    snapshot_id_ = snapshot_id;
    return *this;
  }

  PuffinFileBuilder& AppendBlob(std::string blob_data, std::map<std::string, std::string> properties,
                                std::vector<int32_t> fields, std::string blob_type) {
    Blob blob{.data = std::move(blob_data),
              .properties = std::move(properties),
              .fields = std::move(fields),
              .blob_type = std::move(blob_type)};
    blobs_.emplace_back(std::move(blob));
    return *this;
  }

 private:
  struct Blob {
    std::string data;
    std::map<std::string, std::string> properties;
    std::vector<int32_t> fields;
    std::string blob_type;
  };

  std::vector<Blob> blobs_;
  std::optional<int64_t> sequence_number_;
  std::optional<int64_t> snapshot_id_;
};

}  // namespace iceberg
