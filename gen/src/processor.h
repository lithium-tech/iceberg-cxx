#pragma once

#include <arrow/status.h>

#include <memory>

#include "gen/src/batch.h"

namespace gen {

class IProcessor {
 public:
  virtual arrow::Status Process(BatchPtr) = 0;

  virtual ~IProcessor() = default;
};

class RoundRobinProcessor : public IProcessor {
 public:
  RoundRobinProcessor(std::vector<std::shared_ptr<IProcessor>> processors) : processors_(std::move(processors)) {}

  arrow::Status Process(BatchPtr batch) override {
    ARROW_RETURN_NOT_OK(processors_[processor_to_use_]->Process(batch));
    if (++processor_to_use_ == processors_.size()) {
      processor_to_use_ = 0;
    }
    return arrow::Status::OK();
  }

 private:
  uint32_t processor_to_use_ = 0;
  std::vector<std::shared_ptr<IProcessor>> processors_;
};

class AllProcessor : public IProcessor {
 public:
  explicit AllProcessor() {}

  arrow::Status Process(BatchPtr batch) override {
    for (auto& processor : processors_) {
      ARROW_RETURN_NOT_OK(processor->Process(batch));
    }
    return arrow::Status::OK();
  }

  void AttachChild(std::shared_ptr<IProcessor> child) { processors_.push_back(child); }

 private:
  std::vector<std::shared_ptr<IProcessor>> processors_;
};

}  // namespace gen
