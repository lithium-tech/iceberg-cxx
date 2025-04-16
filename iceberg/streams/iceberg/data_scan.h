#pragma once

#include <memory>
#include <string>

#include "arrow/status.h"
#include "iceberg/streams/arrow/error.h"
#include "iceberg/streams/iceberg/data_entries_meta_stream.h"
#include "iceberg/streams/iceberg/iceberg_batch.h"

namespace iceberg {

class DataScanner : public IcebergStream {
 public:
  class IIcebergStreamBuilder {
   public:
    virtual IcebergStreamPtr Build(const AnnotatedDataPath&) = 0;

    virtual ~IIcebergStreamBuilder() = default;
  };

  explicit DataScanner(AnnotatedDataPathStreamPtr meta_stream, std::shared_ptr<IIcebergStreamBuilder> stream_builder)
      : meta_stream_(meta_stream), data_stream_builder_(stream_builder) {
    Ensure(meta_stream != nullptr, std::string(__PRETTY_FUNCTION__) + ": meta_stream is nullptr");
    Ensure(stream_builder != nullptr, std::string(__PRETTY_FUNCTION__) + ": stream_builder is nullptr");
  }

  std::shared_ptr<IcebergBatch> ReadNext() override {
    while (true) {
      if (!current_data_stream_) {
        auto meta = meta_stream_->ReadNext();
        if (!meta) {
          return nullptr;
        }
        current_data_stream_ = data_stream_builder_->Build(*meta);
        Ensure(current_data_stream_ != nullptr, std::string(__PRETTY_FUNCTION__) + ": current_data_stream is nullptr");
      }
      auto batch = current_data_stream_->ReadNext();
      if (!batch) {
        current_data_stream_.reset();
        continue;
      }
      return batch;
    }
  }

 private:
  AnnotatedDataPathStreamPtr meta_stream_;
  std::shared_ptr<IIcebergStreamBuilder> data_stream_builder_;

  IcebergStreamPtr current_data_stream_;
};

}  // namespace iceberg
