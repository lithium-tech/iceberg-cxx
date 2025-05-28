#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "arrow/status.h"
#include "iceberg/equality_delete/delete.h"
#include "iceberg/equality_delete/utils.h"

namespace iceberg {

class SizedPtr {
 public:
  SizedPtr() = default;
  SizedPtr(uint8_t* data, uint64_t size)
      : data_with_size_(std::bit_cast<uint8_t*>(std::bit_cast<uint64_t>(data) + (size << kDataBits)),
                        SizedPtrDeleter()) {}

  SizedPtr(const SizedPtr&) = default;
  SizedPtr& operator=(const SizedPtr&) = default;

  SizedPtr(SizedPtr&&) = default;
  SizedPtr& operator=(SizedPtr&&) = default;

  inline uint8_t* Data() const {
    return std::bit_cast<uint8_t*>(std::bit_cast<uint64_t>(data_with_size_.get()) & kDataMask);
  }

  inline uint32_t Size() const { return std::bit_cast<uint64_t>(data_with_size_.get()) >> kDataBits; }

 private:
  class SizedPtrDeleter {
   public:
    inline void operator()(uint8_t* ptr) { delete[] std::bit_cast<uint8_t*>(std::bit_cast<uint64_t>(ptr) & kDataMask); }
  };

  friend SizedPtrDeleter;

  static constexpr uint64_t kDataBits = 48;
  static constexpr uint64_t kDataMask = ((1ull << kDataBits) - 1);

  std::shared_ptr<uint8_t[]> data_with_size_ = nullptr;
};

class GenericDeleteKey {
 public:
  GenericDeleteKey(const std::vector<std::shared_ptr<arrow::Array>>& arrays, int row,
                   const std::shared_ptr<MemoryState>& shared_state);

  inline bool operator==(const GenericDeleteKey& other) const {
    return sized_ptr_.Size() == other.sized_ptr_.Size() &&
           std::memcmp(sized_ptr_.Data(), other.sized_ptr_.Data(), sized_ptr_.Size()) == 0;
  }

  template <typename H>
  friend H AbslHashValue(H h, const GenericDeleteKey& key) {
    return H::combine(std::move(h),
                      std::string_view(reinterpret_cast<const char*>(key.sized_ptr_.Data()), key.sized_ptr_.Size()));
  }

  size_t Size() const { return sized_ptr_.Size(); }

  ~GenericDeleteKey() {
    if (shared_state_) {
      shared_state_->Deallocate<uint32_t>();
      shared_state_->DeallocateArray<uint8_t>(sized_ptr_.Size());
    }
  }

 private:
  SizedPtr sized_ptr_;
  std::shared_ptr<MemoryState> shared_state_;
};

class GenericEqualityDelete : public EqualityDelete {
 public:
  explicit GenericEqualityDelete(const std::shared_ptr<MemoryState>& shared_state)
      : values_(shared_state), shared_state_(shared_state) {}

  arrow::Status Add(const std::vector<std::shared_ptr<arrow::Array>>& arrays, uint64_t rows_count,
                    Layer delete_layer) override;

  size_t Size() const override;

  bool IsDeleted(const std::vector<std::shared_ptr<arrow::Array>>& arrays, uint64_t row,
                 Layer data_layer) const override;

 private:
  safe::FlatHashMap<GenericDeleteKey, Layer> values_;
  std::shared_ptr<MemoryState> shared_state_;
};

}  // namespace iceberg
