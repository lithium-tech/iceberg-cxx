#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "arrow/type.h"
#include "gen/src/program.h"
#include "gen/src/table.h"
#include "gen/tpch/generators.h"
#include "gen/tpch/text.h"

namespace gen {

struct SupplierTable : public Table {
  static constexpr std::string_view kSuppkey = "s_suppkey";
  static constexpr std::string_view kName = "s_name";
  static constexpr std::string_view kAddress = "s_address";
  static constexpr std::string_view kNationkey = "s_nationkey";
  static constexpr std::string_view kPhone = "s_phone";
  static constexpr std::string_view kAcctbal = "s_acctbal";
  static constexpr std::string_view kComment = "s_comment";

  std::shared_ptr<arrow::Schema> MakeArrowSchema() const override;

  std::string Name() const override { return "supplier"; }
};

Program MakeSupplierProgram(const tpch::text::Text& text, RandomDevice& random_device, const int32_t scale_factor,
                            int64_t min_rownum);

struct PartTable : public Table {
  static constexpr std::string_view kPartkey = "p_partkey";
  static constexpr std::string_view kName = "p_name";
  static constexpr std::string_view kMfgr = "p_mfgr";
  static constexpr std::string_view kBrand = "p_brand";
  static constexpr std::string_view kType = "p_type";
  static constexpr std::string_view kSize = "p_size";
  static constexpr std::string_view kContainer = "p_container";
  static constexpr std::string_view kRetailprice = "p_retailprice";
  static constexpr std::string_view kComment = "p_comment";

  std::shared_ptr<arrow::Schema> MakeArrowSchema() const override;

  std::string Name() const override { return "part"; }
};

struct PartsuppTable : public Table {
  static constexpr std::string_view kPartkey = "ps_partkey";
  static constexpr std::string_view kSuppkey = "ps_suppkey";
  static constexpr std::string_view kAvailqty = "ps_availqty";
  static constexpr std::string_view kSupplycost = "ps_supplycost";
  static constexpr std::string_view kComment = "ps_comment";

  std::shared_ptr<arrow::Schema> MakeArrowSchema() const override;

  std::string Name() const override { return "partsupp"; }
};

Program MakePartAndPartsuppProgram(const tpch::text::Text& text, RandomDevice& random_device,
                                   const int32_t scale_factor, int64_t min_rownum);

struct CustomerTable : public Table {
  static constexpr std::string_view kCustkey = "c_custkey";
  static constexpr std::string_view kName = "c_name";
  static constexpr std::string_view kAddress = "c_address";
  static constexpr std::string_view kNationkey = "c_nationkey";
  static constexpr std::string_view kPhone = "c_phone";
  static constexpr std::string_view kAcctbal = "c_acctbal";
  static constexpr std::string_view kMktsegment = "c_mktsegment";
  static constexpr std::string_view kComment = "c_comment";

  std::shared_ptr<arrow::Schema> MakeArrowSchema() const override;

  std::string Name() const override { return "customer"; }
};

Program MakeCustomerProgram(const tpch::text::Text& text, RandomDevice& random_device, const int32_t scale_factor,
                            int64_t min_rownum);

struct OrdersTable : public Table {
  static constexpr std::string_view kOrderkey = "o_orderkey";
  static constexpr std::string_view kCustkey = "o_custkey";
  static constexpr std::string_view kOrderstatus = "o_orderstatus";
  static constexpr std::string_view kTotalprice = "o_totalprice";
  static constexpr std::string_view kOrderdate = "o_orderdate";
  static constexpr std::string_view kOrderpriority = "o_orderpriority";
  static constexpr std::string_view kClerk = "o_clerk";
  static constexpr std::string_view kShippriority = "o_shippriority";
  static constexpr std::string_view kComment = "o_comment";

  std::shared_ptr<arrow::Schema> MakeArrowSchema() const override;

  std::string Name() const override { return "orders"; }
};

struct LineitemTable : public Table {
  static constexpr std::string_view kOrderkey = "l_orderkey";
  static constexpr std::string_view kPartkey = "l_partkey";
  static constexpr std::string_view kSuppkey = "l_suppkey";
  static constexpr std::string_view kLinenumber = "l_linenumber";
  static constexpr std::string_view kQuantity = "l_quantity";
  static constexpr std::string_view kExtendedprice = "l_extendedprice";
  static constexpr std::string_view kDiscount = "l_discount";
  static constexpr std::string_view kTax = "l_tax";
  static constexpr std::string_view kReturnflag = "l_returnflag";
  static constexpr std::string_view kLinestatus = "l_linestatus";
  static constexpr std::string_view kShipdate = "l_shipdate";
  static constexpr std::string_view kCommitdate = "l_commitdate";
  static constexpr std::string_view kReceiptdate = "l_receiptdate";
  static constexpr std::string_view kShipinstruct = "l_shipinstruct";
  static constexpr std::string_view kShipmode = "l_shipmode";
  static constexpr std::string_view kComment = "l_comment";

  std::shared_ptr<arrow::Schema> MakeArrowSchema() const override;

  std::string Name() const override { return "lineitem"; }
};

Program MakeOrderAndLineitemProgram(const tpch::text::Text& text, RandomDevice& random_device,
                                    const int32_t scale_factor, int64_t min_rownum);

struct NationTable : public Table {
  static constexpr std::string_view kNationKey = "n_nationkey";
  static constexpr std::string_view kName = "n_name";
  static constexpr std::string_view kRegionKey = "n_regionkey";
  static constexpr std::string_view kComment = "n_comment";

  std::shared_ptr<arrow::Schema> MakeArrowSchema() const override;

  std::string Name() const override { return "nation"; }
};

Program MakeNationProgram(const tpch::text::Text& text, RandomDevice& random_device);

struct RegionTable : public Table {
  static constexpr std::string_view kRegionKey = "r_regionkey";
  static constexpr std::string_view kName = "r_name";
  static constexpr std::string_view kComment = "r_comment";

  std::shared_ptr<arrow::Schema> MakeArrowSchema() const override;

  std::string Name() const override { return "region"; }
};

Program MakeRegionProgram(const tpch::text::Text& text, RandomDevice& random_device);

}  // namespace gen
