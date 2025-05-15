#pragma once

#include <cstdint>

#include "theta_sketch.hpp"

namespace stats {

class ThetaDistinctCounter {
 public:
  ThetaDistinctCounter()
      : sketch_([]() {
          datasketches::update_theta_sketch::builder builder;
          return builder.build();
        }()) {}

  void AppendValue(const int64_t value) { sketch_.update(value); }
  void AppendValue(const void* data, uint64_t size) { sketch_.update(data, size); }

  uint64_t GetDistinctValuesCount() const { return sketch_.get_estimate(); }

  datasketches::update_theta_sketch& GetSketch() { return sketch_; }
  const datasketches::update_theta_sketch& GetSketch() const { return sketch_; }

 private:
  datasketches::update_theta_sketch sketch_;
};

}  // namespace stats
