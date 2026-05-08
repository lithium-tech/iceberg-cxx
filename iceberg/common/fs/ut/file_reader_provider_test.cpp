#include <filesystem>
#include <fstream>
#include <map>
#include <roaring.hh>

#include "gtest/gtest.h"
#include "iceberg/common/fs/file_reader_provider_impl.h"
#include "iceberg/common/fs/filesystem_provider_impl.h"
#include "iceberg/deletion_vector.h"
#include "iceberg/puffin.h"
#include "iceberg/test_utils/scoped_temp_dir.h"

namespace iceberg {
namespace {

TEST(FileReaderProviderTest, OpenDeletionVector) {
  PuffinFile::Footer::BlobMetadata dummy_meta;

  roaring::Roaring64Map bitmap;
  bitmap.add(static_cast<uint64_t>(100));

  dummy_meta.properties[std::string(properties_names::kCardinality)] = std::to_string(bitmap.cardinality());
  dummy_meta.properties[std::string(properties_names::kReferencedDataFile)] = "data.parquet";
  dummy_meta.type = std::string(DeletionVector::kBlobType);

  DeletionVector dv(dummy_meta, std::move(bitmap));
  std::string blob_data = dv.GetBlob();
  auto properties = dv.GetProperties();

  PuffinFileBuilder builder;
  builder.SetSnapshotId(1);
  builder.SetSequenceNumber(1);
  builder.AppendBlob(blob_data, properties, {}, std::string(DeletionVector::kBlobType));

  PuffinFile puffin_file = std::move(builder).Build();
  std::string payload = puffin_file.GetPayload();

  ScopedTempDir dir;
  std::string filename = dir.path() / "test.puffin";
  {
    std::ofstream out(filename, std::ios::binary);
    out.write(payload.data(), payload.size());
  }

  auto footer = puffin_file.GetFooter().GetDeserializedFooter();
  ASSERT_EQ(footer.blobs.size(), 1);
  auto blob_meta = footer.blobs[0];

  std::map<std::string, std::shared_ptr<IFileSystemGetter>> getters;
  getters.emplace("file", std::make_shared<LocalFileSystemGetter>());

  auto fs_provider = std::make_shared<FileSystemProvider>(std::move(getters));
  auto provider = MakeFileReaderProvider(fs_provider);

  auto opened_dv_res = provider->OpenDeletionVector("file://" + filename, blob_meta.offset, blob_meta.length);
  ASSERT_TRUE(opened_dv_res.ok()) << opened_dv_res.status().ToString();
  auto opened_dv = opened_dv_res.ValueOrDie();

  EXPECT_TRUE(opened_dv->Contains(100));
  EXPECT_FALSE(opened_dv->Contains(10));
  EXPECT_EQ(opened_dv->Cardinality(), 1);
}

}  // namespace
}  // namespace iceberg
