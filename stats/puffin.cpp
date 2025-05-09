#include "stats/puffin.h"

#include "stats/datasketch/distinct_theta.h"
#include "theta_sketch.hpp"

namespace stats {

void SketchesToPuffin(const stats::AnalyzeResult& result, iceberg::PuffinFileBuilder& puffin_file_builder) {
  for (const auto& [name, skethces] : result.sketches) {
    if (!skethces.counter) {
      continue;
    }
    const auto& impl = skethces.counter->GetSketch();
    if (auto sketch = std::get_if<stats::ThetaDistinctCounter>(&impl)) {
      auto data = [sketch]() {
        const auto& raw_sketch = sketch->GetSketch();
        datasketches::compact_theta_sketch final_sketch(raw_sketch, raw_sketch.is_ordered());
        std::stringstream ss;
        final_sketch.serialize(ss);
        return ss.str();
      }();
      int64_t ndv = sketch->GetDistinctValuesCount();
      std::map<std::string, std::string> properties;
      properties["ndv"] = std::to_string(ndv);
      puffin_file_builder.AppendBlob(std::move(data), std::move(properties), {skethces.field_id},
                                     "apache-datasketches-theta-v1");
    }
  }
}

iceberg::Statistics PuffinInfoToStatistics(const iceberg::PuffinFile& puffin_file, const std::string& puffin_file_path,
                                           int64_t snapshot_id) {
  const auto& footer = puffin_file.GetFooter();

  iceberg::Statistics stats;
  stats.statistics_path = puffin_file_path;
  stats.snapshot_id = snapshot_id;
  stats.file_footer_size_in_bytes = footer.GetPayload().size();
  stats.file_size_in_bytes = puffin_file.GetPayload().size();

  auto footer_info = footer.GetDeserializedFooter();
  for (const auto& blob : footer_info.blobs) {
    iceberg::BlobMetadata result_blob;
    result_blob.field_ids = blob.fields;
    result_blob.properties = blob.properties;
    result_blob.sequence_number = blob.sequence_number;
    result_blob.type = blob.type;
    result_blob.snapshot_id = blob.snapshot_id;

    stats.blob_metadata.emplace_back(std::move(result_blob));
  }

  return stats;
}

}  // namespace stats
