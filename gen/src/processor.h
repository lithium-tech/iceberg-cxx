#pragma once

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

  virtual arrow::Status Process(BatchPtr batch) {
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

}  // namespace gen
