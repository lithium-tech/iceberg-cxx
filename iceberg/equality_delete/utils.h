#pragma once

#include <limits>
#include <memory>
#include <new>
#include <optional>
#include <string>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "arrow/array.h"

namespace iceberg {

class MemoryState {
 public:
  explicit MemoryState(std::optional<size_t> max_size, bool throw_if_memory_limit_exceeded)
      : max_size_(max_size), throw_on_alloc_exceeded_(throw_if_memory_limit_exceeded) {}

  template <typename T>
  void Allocate() const {
    auto status = AllocateCommon(sizeof(T));
    if (!status.ok()) {
      throw std::bad_alloc();
    }
  }

  template <typename T>
  void AllocateArray(size_t num_elems) const {
    auto status = AllocateCommon(sizeof(T) * num_elems);
    if (!status.ok()) {
      throw std::bad_alloc();
    }
  }

  template <typename T>
  void Deallocate() const {
    allocated_size_ -= sizeof(T);
  }

  template <typename T>
  void DeallocateArray(size_t num_elems) const {
    allocated_size_ -= num_elems * sizeof(T);
  }

  size_t Allocated() const { return allocated_size_; }

  template <typename T>
  arrow::Status AllocateWithStatus() const noexcept {
    return AllocateCommon(sizeof(T));
  }

  template <typename T>
  arrow::Status AllocateArrayWithStatus(size_t num_elems) const noexcept {
    return AllocateCommon(sizeof(T) * num_elems);
  }

  inline void SetExceptionFlag(bool flag) {
    allow_throw_ = flag;
    if (flag && limit_exceeded_ && throw_on_alloc_exceeded_) {
      throw std::bad_alloc();
    }
  }

  size_t RemainingAllocateSize() const {
    if (max_size_) {
      return *max_size_ - allocated_size_;
    } else {
      return std::numeric_limits<size_t>::max();
    }
  }

  void CheckLimitsBeforeAllocation(size_t predicted_ealloc_size) {
    if (predicted_ealloc_size > RemainingAllocateSize() && throw_on_alloc_exceeded_) {
      throw std::bad_alloc();
    }
  }

 private:
  arrow::Status AllocateCommon(size_t bytes) const {
    if (max_size_ && *max_size_ - allocated_size_ < static_cast<size_t>(bytes)) {
      if (throw_on_alloc_exceeded_ && allow_throw_) {
        return arrow::Status::ExecutionError("Error to allocate " + std::to_string(bytes) + " bytes");
      }
      limit_exceeded_ = true;
    }
    allocated_size_ += bytes;
    return arrow::Status::OK();
  }

  const std::optional<size_t> max_size_;
  bool allow_throw_ = true;
  mutable bool limit_exceeded_ = false;
  mutable size_t allocated_size_ = 0;
  bool throw_on_alloc_exceeded_;
};

template <typename T>
class LimitedAllocator : public std::allocator<T> {
 public:
  explicit LimitedAllocator(const std::shared_ptr<MemoryState>& state) : std::allocator<T>(), state_(state) {}

  LimitedAllocator(const LimitedAllocator& other) : std::allocator<T>(other) { state_ = other.state_; }

  template <typename U>
  LimitedAllocator(const LimitedAllocator<U>& other) : std::allocator<T>(other) {
    state_ = other.state_;
  }

  template <typename U>
  LimitedAllocator(LimitedAllocator<U>&& other) : std::allocator<T>(other) {
    state_ = other.state_;
  }

  LimitedAllocator& operator=(const LimitedAllocator& other) {
    if (this != &other) {
      std::allocator<T>::operator=(other);
      state_ = other.state_;
    }
    return *this;
  }

  LimitedAllocator& operator=(LimitedAllocator&& other) noexcept {
    if (this != &other) {
      std::allocator<T>::operator=(std::move(other));
      state_ = std::move(other.state_);
    }
    return *this;
  }

  T* allocate(size_t n) {
    state_->AllocateArray<T>(n);
    return std::allocator<T>::allocate(n);
  }

  void deallocate(T* p, size_t n) {
    std::allocator<T>::deallocate(p, n);
    state_->DeallocateArray<T>(n);
  }

  std::shared_ptr<MemoryState> State() const { return state_; }

 public:
  std::shared_ptr<MemoryState> state_;
};

namespace safe {

class ExceptionFlagGuard {
 public:
  explicit ExceptionFlagGuard(std::shared_ptr<MemoryState> shared_state, bool initial_state)
      : shared_state_(shared_state), initial_state_(initial_state) {
    shared_state_->SetExceptionFlag(initial_state_);
  }

  ~ExceptionFlagGuard() { shared_state_->SetExceptionFlag(!initial_state_); }

 private:
  std::shared_ptr<MemoryState> shared_state_;
  bool initial_state_;
};

// exception safe wrapper for absl::flat_hash_set
template <typename Key, typename Value>
class FlatHashMap {
 public:
  using T = std::pair<const Key, Value>;

  explicit FlatHashMap(const std::shared_ptr<MemoryState>& shared_state)
      : values_(LimitedAllocator<T>(shared_state)), shared_state_(shared_state) {}

  void Set(Key key, Value value) {
    size_t predicted_realloc_size = sizeof(T);
    if (values_.size() + 1 > values_.max_load_factor() * values_.capacity()) {
      predicted_realloc_size = 2 * values_.capacity() * sizeof(T);
    }

    shared_state_->CheckLimitsBeforeAllocation(predicted_realloc_size);
    {
      ExceptionFlagGuard guard(shared_state_, false);
      values_[std::move(key)] = value;
    }
  }

  std::optional<Value> Get(const Key& key) const {
    auto it = values_.find(key);
    if (it == values_.end()) {
      return std::nullopt;
    }
    return it->second;
  }

  size_t Size() const { return values_.size(); }

  void Reserve(size_t size) { values_.reserve(size); }

 private:
  using DefaultHasher = absl::container_internal::hash_default_hash<Key>;
  using DefaultComparator = absl::container_internal::hash_default_eq<Key>;

  absl::flat_hash_map<Key, Value, DefaultHasher, DefaultComparator, LimitedAllocator<T>> values_;

  std::shared_ptr<MemoryState> shared_state_;
};

template <typename Key, typename Value>
void UpdateMax(FlatHashMap<Key, Value>& map, Key key, Value value) {
  auto old_value = map.Get(key);
  if (!old_value) {
    map.Set(std::move(key), value);
  } else {
    map.Set(std::move(key), std::max(*old_value, value));
  }
}

}  // namespace safe

}  // namespace iceberg
