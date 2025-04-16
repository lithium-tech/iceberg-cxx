#pragma once

#include <string>
#include <utility>
#include <vector>

#include "iceberg/streams/arrow/stream.h"
#include "iceberg/streams/iceberg/plan.h"

namespace iceberg {

class AnnotatedDataPath : public PartitionLayerFile {
 public:
  struct Segment {
    int64_t offset;
    int64_t length;
  };

  AnnotatedDataPath() = delete;
  AnnotatedDataPath(PartitionLayerFile s, std::vector<Segment> seg)
      : PartitionLayerFile(std::move(s)), segments_(std::move(seg)) {}

 public:
  const std::vector<Segment>& GetSegments() const { return segments_; }

 protected:
  std::vector<Segment> segments_;
};

using IAnnotatedDataPathStream = IStream<AnnotatedDataPath>;
using AnnotatedDataPathStreamPtr = StreamPtr<AnnotatedDataPath>;

}  // namespace iceberg
