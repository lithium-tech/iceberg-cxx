#include "iceberg/common/fs/url.h"

#include "gtest/gtest.h"

namespace iceberg {
namespace {

TEST(UrlTest, EmptyUrl) {
  auto components = SplitUrl("");
  EXPECT_EQ(components.schema, "");
  EXPECT_EQ(components.location, "");
  EXPECT_EQ(components.path, "");
  EXPECT_EQ(components.params.size(), 0);
}

TEST(UrlTest, SchemaPath) {
  auto components = SplitUrl("file:///tmp/num");
  EXPECT_EQ(components.schema, "file");
  EXPECT_EQ(components.location, "");
  EXPECT_EQ(components.path, "/tmp/num");
  EXPECT_EQ(components.params.size(), 0);
}

TEST(UrlTest, SchemaLocationPath) {
  auto components = SplitUrl("teapot://localhost:1234/namespace.table");
  EXPECT_EQ(components.schema, "teapot");
  EXPECT_EQ(components.location, "localhost:1234");
  EXPECT_EQ(components.path, "/namespace.table");
  EXPECT_EQ(components.params.size(), 0);
}

TEST(UrlTest, SchemaLocationRootPath) {
  auto components = SplitUrl("teapot://localhost:1234/");
  EXPECT_EQ(components.schema, "teapot");
  EXPECT_EQ(components.location, "localhost:1234");
  EXPECT_EQ(components.path, "/");
  EXPECT_EQ(components.params.size(), 0);
}

TEST(UrlTest, SchemaLocation) {
  auto components = SplitUrl("teapot://localhost:1234");
  EXPECT_EQ(components.schema, "teapot");
  EXPECT_EQ(components.location, "localhost:1234");
  EXPECT_EQ(components.path, "");
  EXPECT_EQ(components.params.size(), 0);
}

TEST(UrlTest, Location) {
  auto components = SplitUrl("localhost:1234");
  EXPECT_EQ(components.schema, "");
  EXPECT_EQ(components.location, "localhost:1234");
  EXPECT_EQ(components.path, "");
  EXPECT_EQ(components.params.size(), 0);
}

TEST(UrlTest, LocationPath) {
  auto components = SplitUrl("localhost:1234/path/to/file");
  EXPECT_EQ(components.schema, "");
  EXPECT_EQ(components.location, "localhost:1234");
  EXPECT_EQ(components.path, "/path/to/file");
  EXPECT_EQ(components.params.size(), 0);
}

TEST(UrlTest, Path) {
  auto components = SplitUrl("/path/to/file");
  EXPECT_EQ(components.schema, "");
  EXPECT_EQ(components.location, "");
  EXPECT_EQ(components.path, "/path/to/file");
  EXPECT_EQ(components.params.size(), 0);
}

TEST(UrlTest, LocationQuery) {
  auto components = SplitUrl("localhost:1234?q");
  EXPECT_EQ(components.schema, "");
  EXPECT_EQ(components.location, "localhost:1234");
  EXPECT_EQ(components.path, "");
  ASSERT_EQ(components.params.size(), 1);
  EXPECT_EQ(components.params[0].first, "q");
  EXPECT_EQ(components.params[0].second, "");
}

TEST(UrlTest, LocationPathQuery) {
  auto components = SplitUrl("localhost:1234/path/to?q");
  EXPECT_EQ(components.schema, "");
  EXPECT_EQ(components.location, "localhost:1234");
  EXPECT_EQ(components.path, "/path/to");
  ASSERT_EQ(components.params.size(), 1);
  EXPECT_EQ(components.params[0].first, "q");
  EXPECT_EQ(components.params[0].second, "");
}

TEST(UrlTest, LocationQueryMultiparam) {
  auto components = SplitUrl("localhost:1234?q=2&param=value");
  EXPECT_EQ(components.schema, "");
  EXPECT_EQ(components.location, "localhost:1234");
  EXPECT_EQ(components.path, "");
  ASSERT_EQ(components.params.size(), 2);
  EXPECT_EQ(components.params[0].first, "q");
  EXPECT_EQ(components.params[0].second, "2");
  EXPECT_EQ(components.params[1].first, "param");
  EXPECT_EQ(components.params[1].second, "value");
}

}  // namespace
}  // namespace iceberg
