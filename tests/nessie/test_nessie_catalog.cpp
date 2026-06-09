#include "gtest/gtest.h"
#include "iceberg/catalog.h"
#include "iceberg/schema.h"
#include "iceberg/table_metadata.h"
#include "iceberg/tea_nessie_catalog.h"

namespace iceberg {

TEST(Catalog, Nessie) {
  auto nessie_catalog = ice_tea::NessieCatalog("127.0.0.1", 19120);

  auto table_identifier = catalog::TableIdentifier{.db = "default", .name = "test"};

  EXPECT_EQ(nessie_catalog.LoadTable(table_identifier)->Location(), "file:///some_snap");
}

}  // namespace iceberg
