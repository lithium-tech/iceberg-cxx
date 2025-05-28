#include "iceberg/deletion_vector.h"

#include <fstream>

#include "gtest/gtest.h"

namespace iceberg {
namespace {
using BlobMetadata = PuffinFile::Footer::BlobMetadata;
BlobMetadata MakeBlobMetadata(const roaring::Roaring64Map& bitmap) {
  BlobMetadata res;
  res.type = DeletionVector::kBlobType;
  res.length = bitmap.getSizeInBytes() + DeletionVector::kMagicSequence.size() + 2 * sizeof(uint32_t);
  res.properties[std::string(properties_names::kCardinality)] = std::to_string(bitmap.cardinality());
  res.properties[std::string(properties_names::kReferencedDataFile)] = "";
  return res;
}
}  // namespace

TEST(DeletionVector, GetElemsCheck) {
  roaring::Roaring64Map roaring_map;
  roaring_map.add((uint64_t)1);
  roaring_map.add((uint64_t)7);
  roaring_map.add((uint64_t)9);

  DeletionVector deletion_vector(MakeBlobMetadata(roaring_map), roaring_map);

  EXPECT_EQ(deletion_vector.Cardinality(), 3);
  EXPECT_EQ(deletion_vector.GetElems(0, std::numeric_limits<uint64_t>::max()), std::vector<uint64_t>({1, 7, 9}));

  EXPECT_EQ(deletion_vector.Cardinality(4, 10), 2);
  EXPECT_EQ(deletion_vector.GetElems(4, 10), std::vector<uint64_t>({7, 9}));

  EXPECT_EQ(deletion_vector.Cardinality(3, 5), 0);
  EXPECT_EQ(deletion_vector.GetElems(3, 5), std::vector<uint64_t>());
}

TEST(DeletionVector, GetElemsCheckEdgeCases) {
  roaring::Roaring64Map roaring_map;
  roaring_map.add((uint64_t)1);
  roaring_map.add((uint64_t)7);
  roaring_map.add((uint64_t)9);

  DeletionVector deletion_vector(MakeBlobMetadata(roaring_map), roaring_map);

  EXPECT_EQ(deletion_vector.Cardinality(1, 7), 2);
  EXPECT_EQ(deletion_vector.GetElems(1, 7), std::vector<uint64_t>({1, 7}));

  EXPECT_EQ(deletion_vector.Cardinality(2, 7), 1);
  EXPECT_EQ(deletion_vector.GetElems(2, 7), std::vector<uint64_t>({7}));

  EXPECT_EQ(deletion_vector.Cardinality(1, 6), 1);
  EXPECT_EQ(deletion_vector.GetElems(1, 6), std::vector<uint64_t>({1}));

  EXPECT_EQ(deletion_vector.Cardinality(2, 6), 0);
  EXPECT_EQ(deletion_vector.GetElems(2, 6), std::vector<uint64_t>());
}

TEST(DeletionVector, DeletionVectorFromBlob) {
  roaring::Roaring64Map roaring_map;
  roaring_map.add((uint64_t)1);
  roaring_map.add((uint64_t)7);
  roaring_map.add((uint64_t)9);

  DeletionVector deletion_vector1(MakeBlobMetadata(roaring_map), roaring_map);
  DeletionVector deletion_vector2(MakeBlobMetadata(roaring_map), deletion_vector1.GetBlob());

  EXPECT_EQ(deletion_vector2.GetElems(0, std::numeric_limits<uint64_t>::max()), std::vector<uint64_t>({1, 7, 9}));
}

TEST(DeletionVector, AddManySpan) {
  roaring::Roaring64Map roaring_map;
  DeletionVector deletion_vector(MakeBlobMetadata(roaring_map), roaring_map);
  uint64_t a[3]{1, 7, 9};
  deletion_vector.AddMany(a);

  EXPECT_EQ(deletion_vector.GetElems(0, std::numeric_limits<uint64_t>::max()), std::vector<uint64_t>({1, 7, 9}));
}

TEST(DeletionVector, AddManyVector) {
  roaring::Roaring64Map roaring_map;
  DeletionVector deletion_vector(MakeBlobMetadata(roaring_map), roaring_map);
  std::vector<uint64_t> a({1, 7, 9});
  deletion_vector.AddMany(a);

  EXPECT_EQ(deletion_vector.GetElems(0, std::numeric_limits<uint64_t>::max()), std::vector<uint64_t>({1, 7, 9}));
}

TEST(DeletionVector, Read) {
  std::ifstream input(
      "tables/deletion_vector/deletion_vector_sample/data/"
      "00000-2-766d0c1d-4652-4a59-9721-58fb02898559-00001-deletes.puffin");
  std::stringstream ss;
  ss << input.rdbuf();
  std::string data = ss.str();

  auto puffin_file = PuffinFile::Make(data);
  EXPECT_TRUE(puffin_file.ok());
  EXPECT_EQ(puffin_file->GetFooter().GetBlobsCount(), 1);

  auto blob_metadata = puffin_file->GetFooter().GetBlobMetadata(0);
  auto blob = puffin_file->GetBlob(0);

  auto deletion_vector = DeletionVector(blob_metadata, blob);
  EXPECT_EQ(deletion_vector.Cardinality(0, std::numeric_limits<uint64_t>::max()), 5);

  std::vector<uint64_t> res(5);
  for (int i = 0; i < 5; ++i) {
    res[i] = 2 * i + 1;
  }
  EXPECT_EQ(deletion_vector.GetElems(0, std::numeric_limits<uint64_t>::max()), res);
}

}  // namespace iceberg
