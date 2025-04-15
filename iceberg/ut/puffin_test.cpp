#include "iceberg/puffin.h"

#include <stdexcept>

#include "arrow/status.h"
#include "gtest/gtest.h"

namespace iceberg {
namespace {

TEST(PuffinBuilder, Simple) {
  std::map<std::string, std::string> properties = {{"property-key", "property-value"}};

  PuffinFileBuilder builder;
  builder.SetSnapshotId(1);
  builder.SetSequenceNumber(2);
  builder.AppendBlob("blob-data-1", properties, std::vector<int32_t>{1, 2, 3}, "blob-type");
  builder.AppendBlob("blob-data-2", properties, std::vector<int32_t>{1, 2, 3}, "blob-type");

  PuffinFile file = std::move(builder).Build();
}

TEST(PuffinBuilder, NoSnapshot) {
  std::map<std::string, std::string> properties = {{"property-key", "property-value"}};

  PuffinFileBuilder builder;
  builder.SetSequenceNumber(2);
  builder.AppendBlob("blob-data-1", properties, std::vector<int32_t>{1, 2, 3}, "blob-type");
  builder.AppendBlob("blob-data-2", properties, std::vector<int32_t>{1, 2, 3}, "blob-type");

  try {
    PuffinFile file = std::move(builder).Build();
    FAIL();
  } catch (const std::runtime_error& e) {
    std::string error_message = e.what();
    EXPECT_EQ(error_message, "PuffinFileBuilder: snapshot_id is not set");
  }
}

TEST(PuffinBuilder, NoSequenceNumber) {
  std::map<std::string, std::string> properties = {{"property-key", "property-value"}};

  PuffinFileBuilder builder;
  builder.SetSnapshotId(1);
  builder.AppendBlob("blob-data-1", properties, std::vector<int32_t>{1, 2, 3}, "blob-type");
  builder.AppendBlob("blob-data-2", properties, std::vector<int32_t>{1, 2, 3}, "blob-type");

  try {
    PuffinFile file = std::move(builder).Build();
    FAIL();
  } catch (const std::runtime_error& e) {
    std::string error_message = e.what();
    EXPECT_EQ(error_message, "PuffinFileBuilder: sequence_number is not set");
  }
}

TEST(PuffinBuilder, NoBlobs) {
  PuffinFileBuilder builder;
  builder.SetSnapshotId(1);
  builder.SetSequenceNumber(2);

  try {
    PuffinFile file = std::move(builder).Build();
    FAIL();
  } catch (const std::runtime_error& e) {
    std::string error_message = e.what();
    EXPECT_EQ(error_message, "PuffinFileBuilder: building puffin file without blobs is not supported");
  }
}

TEST(PuffinBuilder, Bytes) {
  using namespace std::string_literals;

  std::string payload =
      "PFA1blob-dataPFA1{\"blobs\":[{\"fields\":[1],\"type\":\"t\",\"compression-codec\":null,\"length\":9,\"offset\":"
      "4,\"properties\":{},\"sequence-number\":2,\"snapshot-id\":1}],\"properties\":{\"created-by\":\"iceberg-cpp\"}}"
      "\xb2\x00\x00\x00\x30\x30\x30\x30PFA1"s;

  PuffinFileBuilder builder;
  builder.SetSnapshotId(1);
  builder.SetSequenceNumber(2);
  builder.AppendBlob("blob-data", {}, std::vector<int32_t>{1}, "t");

  PuffinFile file = std::move(builder).Build();

  EXPECT_EQ(file.GetPayload(), payload);
}

TEST(PuffinBuilder, FooterBytes) {
  using namespace std::string_literals;

  std::string footer_payload =
      "{\"blobs\":[{\"fields\":[1],\"type\":\"t\",\"compression-codec\":null,\"length\":9,\"offset\":4,"
      "\"properties\":{},\"sequence-number\":2,\"snapshot-id\":1}],\"properties\":{\"created-by\":\"iceberg-cpp\"}}";

  PuffinFileBuilder builder;
  builder.SetSnapshotId(1);
  builder.SetSequenceNumber(2);
  builder.AppendBlob("blob-data", {}, std::vector<int32_t>{1}, "t");

  PuffinFile file = std::move(builder).Build();
  auto footer = file.GetFooter();
  EXPECT_EQ(footer.GetPayload(), footer_payload);
  auto deserialized_footer = footer.GetDeserializedFooter();

  std::map<std::string, std::string> expected_properties = {{"created-by", "iceberg-cpp"}};
  EXPECT_EQ(deserialized_footer.properties, expected_properties);

  EXPECT_EQ(deserialized_footer.blobs.size(), 1);
  const auto& blob_info = deserialized_footer.blobs[0];
  EXPECT_EQ(blob_info.compression_codec, std::nullopt);
  EXPECT_EQ(blob_info.fields, std::vector<int32_t>{1});
  EXPECT_EQ(blob_info.length, 9);
  EXPECT_EQ(blob_info.offset, 4);
  EXPECT_EQ(blob_info.sequence_number, 2);
  EXPECT_EQ(blob_info.snapshot_id, 1);
  EXPECT_EQ(blob_info.type, "t");
}

TEST(PuffinFile, FromBytes) {
  using namespace std::string_literals;

  std::string payload =
      "PFA1blob-dataPFA1{\"blobs\":[{\"fields\":[1],\"type\":\"t\",\"compression-codec\":null,\"length\":9,\"offset\":"
      "4,\"properties\":{},\"sequence-number\":2,\"snapshot-id\":1}],\"properties\":{\"created-by\":\"iceberg-cpp\"}}"
      "\xb2\x00\x00\x00\x30\x30\x30\x30PFA1"s;

  auto puffin_file = PuffinFile::Make(payload);
  ASSERT_TRUE(puffin_file.ok());

  EXPECT_EQ(puffin_file->GetPayload(), payload);
}

TEST(PuffinFile, GetBlob) {
  using namespace std::string_literals;

  std::string payload =
      "PFA1blob-dataPFA1{\"blobs\":[{\"fields\":[1],\"type\":\"t\",\"compression-codec\":null,\"length\":9,\"offset\":"
      "4,\"properties\":{},\"sequence-number\":2,\"snapshot-id\":1}],\"properties\":{\"created-by\":\"iceberg-cpp\"}}"
      "\xb2\x00\x00\x00\x30\x30\x30\x30PFA1"s;

  auto puffin_file = PuffinFile::Make(payload);
  ASSERT_TRUE(puffin_file.ok());

  auto blobs_count = puffin_file->GetFooter().GetBlobsCount();
  ASSERT_EQ(blobs_count, 1);
  std::string blob_data = puffin_file->GetBlob(0);
  EXPECT_EQ(blob_data, "blob-data");
}

// TODO(gmusya): check negative cases

}  // namespace
}  // namespace iceberg
