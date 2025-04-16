#pragma once

#include <numeric>
#include <utility>
#include <vector>

namespace iceberg {

template <typename T>
class SelectionVector {
 public:
  explicit SelectionVector(size_t size) { Set(size); }
  explicit SelectionVector(std::vector<T> indices) : indices_(std::move(indices)) {}

  template <typename Iterator>
  size_t KeepIfEqual(const Iterator& begin, const Iterator& end) {
    Iterator current_iter = begin;
    size_t result_index = 0;
    for (size_t index_to_check = 0; index_to_check < indices_.size(); ++index_to_check) {
      const T row_to_check = indices_[index_to_check];
      while (current_iter != end && *current_iter < row_to_check) {
        ++current_iter;
      }
      if (current_iter == end) {
        break;
      }
      bool are_equal = *current_iter == row_to_check;
      if (are_equal) {
        indices_[result_index] = indices_[index_to_check];
        ++result_index;
      }
    }
    size_t removed = indices_.size() - result_index;
    indices_.resize(result_index);
    return removed;
  }

  template <typename Iterator>
  size_t DeleteIfEqual(const Iterator& begin, const Iterator& end) {
    Iterator current_iter = begin;
    size_t result_index = 0;
    for (size_t index_to_check = 0; index_to_check < indices_.size(); ++index_to_check) {
      const T row_to_check = indices_[index_to_check];
      while (current_iter != end && *current_iter < row_to_check) {
        ++current_iter;
      }
      if (current_iter == end) {
        for (size_t i = index_to_check; i < indices_.size(); ++i) {
          indices_[result_index] = indices_[i];
          ++result_index;
        }
        break;
      }
      bool are_equal = *current_iter == row_to_check;
      if (!are_equal) {
        indices_[result_index] = indices_[index_to_check];
        ++result_index;
      }
    }
    size_t removed = indices_.size() - result_index;
    indices_.resize(result_index);
    return removed;
  }

  // returns the number of elements removed
  template <typename Predicate>
  size_t EraseIf(Predicate&& pred) {
    return std::erase_if(indices_, pred);
  }

  void Set(size_t size) {
    indices_.resize(size);
    std::iota(indices_.begin(), indices_.end(), 0);
  }

  size_t Size() const { return indices_.size(); }
  T Index(size_t pos) const { return indices_[pos]; }

  const std::vector<T>& GetVector() const { return indices_; }

 private:
  std::vector<T> indices_;
};

}  // namespace iceberg
