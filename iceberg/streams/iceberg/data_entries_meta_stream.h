#pragma once

#include <optional>
#include <utility>
#include <vector>

#include "iceberg/streams/arrow/stream.h"
#include "iceberg/streams/iceberg/plan.h"
#include "iceberg/tea_scan.h"

namespace iceberg {

class AnnotatedDataPath : public PartitionLayerFile {
 public:
  struct Segment {
    int64_t offset;
    int64_t length;
  };

  AnnotatedDataPath() = delete;
  AnnotatedDataPath(PartitionLayerFile s, std::vector<Segment> seg,
                    std::optional<ice_tea::DeletionVectorInfo> dv_info = std::nullopt)
      : PartitionLayerFile(std::move(s)), segments_(std::move(seg)), dv_info_(std::move(dv_info)) {}

 public:
  const std::vector<Segment>& GetSegments() const { return segments_; }
  const std::optional<ice_tea::DeletionVectorInfo>& GetDeletionVector() const { return dv_info_; }

 protected:
  std::vector<Segment> segments_;
  std::optional<ice_tea::DeletionVectorInfo> dv_info_;
};

using IAnnotatedDataPathStream = IStream<AnnotatedDataPath>;
using AnnotatedDataPathStreamPtr = StreamPtr<AnnotatedDataPath>;

}  // namespace iceberg
