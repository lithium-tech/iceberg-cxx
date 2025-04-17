#include "gtest/gtest.h"
#include "iceberg/catalog.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
#include "iceberg/tea_rest_catalog.h"

namespace iceberg {

TEST(Catalog, REST) {
  auto rest_catalog = ice_tea::RESTCatalog("127.0.0.1", 19120);

  auto table_identifier = catalog::TableIdentifier{.db = "default", .name = "test"};
  auto fake_table_identifier = catalog::TableIdentifier{.db = "default", .name = "fake"};

  EXPECT_TRUE(rest_catalog.TableExists(table_identifier));
  EXPECT_FALSE(rest_catalog.TableExists(fake_table_identifier));

  EXPECT_EQ(rest_catalog.LoadTable(table_identifier)->Location(), "file:///some_snap");
}

}  // namespace iceberg
