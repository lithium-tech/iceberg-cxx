#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "iceberg/streams/arrow/batch_with_row_number.h"
#include "iceberg/streams/arrow/stream.h"

namespace iceberg {

template <typename T>
class MockStream : public IStream<T> {
 public:
  explicit MockStream(std::vector<T> values) : values_(std::move(values)) {}

  std::shared_ptr<T> ReadNext() override {
    if (current_position_ == values_.size()) {
      return nullptr;
    }
    return std::make_shared<T>(std::move(values_.at(current_position_++)));
  }

 private:
  std::vector<T> values_;
  size_t current_position_ = 0;
};

}  // namespace iceberg
