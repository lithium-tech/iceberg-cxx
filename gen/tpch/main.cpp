#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <ostream>
#include <stdexcept>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "arrow/csv/options.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "gen/src/fields.h"
#include "gen/src/generators.h"
#include "gen/src/program.h"
#include "gen/src/table.h"
#include "gen/src/writer.h"
#include "gen/tpch/generators.h"
#include "gen/tpch/list.h"
#include "gen/tpch/text.h"
#include "parquet/schema.h"

using namespace gen;

constexpr int32_t kStartDate = 8035;
constexpr int32_t kCurrentDate = 9298;
constexpr int32_t kEndDate = 10591;

struct SupplierTable : public Table {
  static constexpr std::string_view kSuppkey = "s_suppkey";
  static constexpr std::string_view kName = "s_name";
  static constexpr std::string_view kAddress = "s_address";
  static constexpr std::string_view kNationkey = "s_nationkey";
  static constexpr std::string_view kPhone = "s_phone";
  static constexpr std::string_view kAcctbal = "s_acctbal";
  static constexpr std::string_view kComment = "s_comment";

  std::shared_ptr<arrow::Schema> MakeArrowSchema() const override {
    arrow::FieldVector fields;
    fields.emplace_back(arrow::field(std::string(kSuppkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kName), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kAddress), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kNationkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kPhone), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kAcctbal), arrow::decimal128(10, 2)));
    fields.emplace_back(arrow::field(std::string(kComment), arrow::utf8()));
    return std::make_shared<arrow::Schema>(fields);
  }

  std::string Name() const override { return "supplier"; }
};

Program MakeSupplierProgram(const tpch::text::Text& text, RandomDevice& random_device, const int32_t scale_factor) {
  Program program;
  program.AddAssign(Assignment(SupplierTable::kSuppkey, std::make_shared<UniqueIntegerGenerator<arrow::Int32Type>>()));

  program.AddAssign(
      Assignment("s_suppkey_string", std::make_shared<ToStringGenerator<arrow::Int32Type>>(SupplierTable::kSuppkey)));
  program.AddAssign(
      Assignment("s_suppkey_string_zeros", std::make_shared<tpch::AppendLeadingZerosGenerator>("s_suppkey_string", 9)));

  program.AddAssign(Assignment("s_name_prefix", std::make_shared<ConstantGenerator<arrow::StringType>>("Supplier#")));

  program.AddAssign(Assignment(
      SupplierTable::kName,
      std::make_shared<ConcatenateGenerator>(std::vector<std::string>{"s_name_prefix", "s_suppkey_string_zeros"}, "")));

  program.AddAssign(
      Assignment(SupplierTable::kAddress, std::make_shared<tpch::VStringGenerator>(10, 40, random_device)));

  program.AddAssign(Assignment(SupplierTable::kNationkey,
                               std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(0, 24, random_device)));

  program.AddAssign(Assignment(SupplierTable::kPhone,
                               std::make_shared<tpch::PhoneGenerator>(SupplierTable::kSuppkey, random_device)));

  program.AddAssign(Assignment(
      "acctbal_int", std::make_shared<UniformIntegerGenerator<arrow::Int64Type>>(-99'999, 999'999, random_device)));

  program.AddAssign(Assignment(SupplierTable::kAcctbal,
                               std::make_shared<ToDecimalGenerator<arrow::Int64Type>>("acctbal_int", 10, 2)));

  program.AddAssign(
      Assignment(SupplierTable::kComment, std::make_shared<tpch::supplier::CommentGenerator>(text, random_device)));

  program.AddProjection(Projection(SupplierTable().MakeColumnNames()));

  return program;
}

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

  std::shared_ptr<arrow::Schema> MakeArrowSchema() const override {
    arrow::FieldVector fields;
    fields.emplace_back(arrow::field(std::string(kPartkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kName), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kMfgr), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kBrand), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kType), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kSize), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kContainer), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kRetailprice), arrow::decimal128(10, 2)));
    fields.emplace_back(arrow::field(std::string(kComment), arrow::utf8()));
    return std::make_shared<arrow::Schema>(fields);
  }

  std::string Name() const override { return "part"; }
};

struct PartsuppTable : public Table {
  static constexpr std::string_view kPartkey = "ps_partkey";
  static constexpr std::string_view kSuppkey = "ps_suppkey";
  static constexpr std::string_view kAvailqty = "ps_availqty";
  static constexpr std::string_view kSupplycost = "ps_supplycost";
  static constexpr std::string_view kComment = "ps_comment";

  std::shared_ptr<arrow::Schema> MakeArrowSchema() const override {
    arrow::FieldVector fields;
    fields.emplace_back(arrow::field(std::string(kPartkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kSuppkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kAvailqty), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kSupplycost), arrow::decimal128(10, 2)));
    fields.emplace_back(arrow::field(std::string(kComment), arrow::utf8()));
    return std::make_shared<arrow::Schema>(fields);
  }

  std::string Name() const override { return "partsupp"; }
};

Program MakePartAndPartsuppProgram(const tpch::text::Text& text, RandomDevice& random_device,
                                   const int32_t scale_factor) {
  Program program;

  program.AddAssign(Assignment(PartTable::kPartkey, std::make_shared<UniqueIntegerGenerator<arrow::Int32Type>>()));

  auto p_name_list = tpch::part::GetPNameList();
  auto p_name_generator = std::make_shared<tpch::part::NameGenerator>(*p_name_list, random_device);
  program.AddAssign(Assignment(PartTable::kName, p_name_generator));

  program.AddAssign(Assignment("M", std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(1, 5, random_device)));
  program.AddAssign(Assignment("M_string", std::make_shared<ToStringGenerator<arrow::Int32Type>>("M")));
  program.AddAssign(
      Assignment("Manufacturer#", std::make_shared<ConstantGenerator<arrow::StringType>>("Manufacturer#")));
  program.AddAssign(Assignment(PartTable::kMfgr, std::make_shared<ConcatenateGenerator>(
                                                     std::vector<std::string>{"Manufacturer#", "M_string"}, "")));

  program.AddAssign(Assignment("N", std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(1, 5, random_device)));
  program.AddAssign(Assignment("N_string", std::make_shared<ToStringGenerator<arrow::Int32Type>>("N")));
  program.AddAssign(Assignment("Brand#", std::make_shared<ConstantGenerator<arrow::StringType>>("Brand#")));
  program.AddAssign(Assignment(PartTable::kBrand, std::make_shared<ConcatenateGenerator>(
                                                      std::vector<std::string>{"Brand#", "M_string", "N_string"}, "")));
  program.AddProjection(Projection({PartTable::kPartkey, PartTable::kName, PartTable::kMfgr, PartTable::kBrand}));

  auto types_list = tpch::GetTypesList();
  program.AddAssign(
      Assignment(PartTable::kType, std::make_shared<StringFromListGenerator>(*types_list, random_device)));

  program.AddAssign(
      Assignment(PartTable::kSize, std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(1, 50, random_device)));

  auto container_list = tpch::GetContainersList();
  program.AddAssign(
      Assignment(PartTable::kContainer, std::make_shared<StringFromListGenerator>(*container_list, random_device)));

  program.AddAssign(
      Assignment("retailprice_int", std::make_shared<tpch::part::RetailPriceGenerator>(PartTable::kPartkey)));
  program.AddAssign(Assignment(PartTable::kRetailprice,
                               std::make_shared<ToDecimalGenerator<arrow::Int64Type>>("retailprice_int", 10, 2)));

  program.AddAssign(
      Assignment(PartTable::kComment, std::make_shared<tpch::TextStringGenerator>(text, random_device, 5, 22)));

  program.AddAssign(Assignment("ps_vec_size", std::make_shared<ConstantGenerator<arrow::Int32Type>>(4)));

  program.AddAssign(Assignment("ps_repetition_levels", std::make_shared<RepetitionLevelsGenerator>("ps_vec_size")));

  program.AddAssign(Assignment(PartsuppTable::kPartkey, std::make_shared<CopyGenerator<arrow::Int32Type>>(
                                                            "ps_repetition_levels", PartTable::kPartkey)));

  program.AddAssign(
      Assignment("corresponding_supplier", std::make_shared<PositionWithinArrayGenerator>("ps_repetition_levels", 0)));
  program.AddAssign(
      Assignment(PartsuppTable::kSuppkey, std::make_shared<tpch::partsupp::SuppkeyGenerator>(
                                              PartsuppTable::kPartkey, "corresponding_supplier", scale_factor)));

  program.AddAssign(Assignment(PartsuppTable::kAvailqty,
                               std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(1, 9999, random_device)));

  program.AddAssign(Assignment(
      "supplycost_int", std::make_shared<UniformIntegerGenerator<arrow::Int64Type>>(100, 100000, random_device)));
  program.AddAssign(Assignment(PartsuppTable::kSupplycost,
                               std::make_shared<ToDecimalGenerator<arrow::Int64Type>>("supplycost_int", 10, 2)));

  program.AddAssign(
      Assignment(PartsuppTable::kComment, std::make_shared<tpch::TextStringGenerator>(text, random_device, 49, 198)));

  std::vector<std::string> all_columns = PartTable().MakeColumnNames();
  std::vector<std::string> partsupp_columns = PartsuppTable().MakeColumnNames();
  all_columns.insert(all_columns.end(), partsupp_columns.begin(), partsupp_columns.end());
  program.AddProjection(Projection(all_columns));

  return program;
}

struct CustomerTable : public Table {
  static constexpr std::string_view kCustkey = "c_custkey";
  static constexpr std::string_view kName = "c_name";
  static constexpr std::string_view kAddress = "c_address";
  static constexpr std::string_view kNationkey = "c_nationkey";
  static constexpr std::string_view kPhone = "c_phone";
  static constexpr std::string_view kAcctbal = "c_acctbal";
  static constexpr std::string_view kMktsegment = "c_mktsegment";
  static constexpr std::string_view kComment = "c_comment";

  std::shared_ptr<arrow::Schema> MakeArrowSchema() const override {
    arrow::FieldVector fields;
    fields.emplace_back(arrow::field(std::string(kCustkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kName), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kAddress), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kNationkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kPhone), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kAcctbal), arrow::decimal128(10, 2)));
    fields.emplace_back(arrow::field(std::string(kMktsegment), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kComment), arrow::utf8()));
    return std::make_shared<arrow::Schema>(fields);
  }

  std::string Name() const override { return "customer"; }
};

Program MakeCustomerProgram(const tpch::text::Text& text, RandomDevice& random_device, const int32_t scale_factor) {
  Program program;

  program.AddAssign(Assignment(CustomerTable::kCustkey, std::make_shared<UniqueIntegerGenerator<arrow::Int32Type>>()));

  program.AddAssign(
      Assignment("c_custkey_string", std::make_shared<ToStringGenerator<arrow::Int32Type>>(CustomerTable::kCustkey)));
  program.AddAssign(
      Assignment("c_custkey_string_zeros", std::make_shared<tpch::AppendLeadingZerosGenerator>("c_custkey_string", 9)));
  program.AddAssign(
      Assignment("c_custname_prefix", std::make_shared<ConstantGenerator<arrow::StringType>>("Customer#")));
  program.AddAssign(Assignment(CustomerTable::kName,
                               std::make_shared<ConcatenateGenerator>(
                                   std::vector<std::string>{"c_custname_prefix", "c_custkey_string_zeros"}, "")));

  program.AddAssign(
      Assignment(CustomerTable::kAddress, std::make_shared<tpch::VStringGenerator>(10, 40, random_device)));

  program.AddAssign(Assignment(CustomerTable::kNationkey,
                               std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(0, 24, random_device)));

  program.AddAssign(Assignment(CustomerTable::kPhone,
                               std::make_shared<tpch::PhoneGenerator>(CustomerTable::kNationkey, random_device)));

  program.AddAssign(Assignment(
      "acctbal_int", std::make_shared<UniformIntegerGenerator<arrow::Int64Type>>(-99999, 999999, random_device)));
  program.AddAssign(Assignment(CustomerTable::kAcctbal,
                               std::make_shared<ToDecimalGenerator<arrow::Int64Type>>("acctbal_int", 10, 2)));

  auto segments_list = tpch::GetSegmentsList();

  program.AddAssign(
      Assignment(CustomerTable::kMktsegment, std::make_shared<StringFromListGenerator>(*segments_list, random_device)));

  program.AddAssign(
      Assignment(CustomerTable::kComment, std::make_shared<tpch::TextStringGenerator>(text, random_device, 29, 116)));

  program.AddProjection(Projection(CustomerTable().MakeColumnNames()));

  return program;
}

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

  std::shared_ptr<arrow::Schema> MakeArrowSchema() const override {
    arrow::FieldVector fields;
    fields.emplace_back(arrow::field(std::string(kOrderkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kCustkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kOrderstatus), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kTotalprice), arrow::decimal128(10, 2)));
    fields.emplace_back(arrow::field(std::string(kOrderdate), arrow::date32()));
    fields.emplace_back(arrow::field(std::string(kOrderpriority), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kClerk), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kShippriority), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kComment), arrow::utf8()));
    return std::make_shared<arrow::Schema>(fields);
  }

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

  std::shared_ptr<arrow::Schema> MakeArrowSchema() const override {
    arrow::FieldVector fields;
    fields.emplace_back(arrow::field(std::string(kOrderkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kPartkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kSuppkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kLinenumber), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kQuantity), arrow::decimal128(10, 2)));
    fields.emplace_back(arrow::field(std::string(kExtendedprice), arrow::decimal128(10, 2)));
    fields.emplace_back(arrow::field(std::string(kDiscount), arrow::decimal128(10, 2)));
    fields.emplace_back(arrow::field(std::string(kTax), arrow::decimal128(10, 2)));
    fields.emplace_back(arrow::field(std::string(kReturnflag), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kLinestatus), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kShipdate), arrow::date32()));
    fields.emplace_back(arrow::field(std::string(kCommitdate), arrow::date32()));
    fields.emplace_back(arrow::field(std::string(kReceiptdate), arrow::date32()));
    fields.emplace_back(arrow::field(std::string(kShipinstruct), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kShipmode), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kComment), arrow::utf8()));
    return std::make_shared<arrow::Schema>(fields);
  }

  std::string Name() const override { return "lineitem"; }
};

Program MakeOrderAndLineitemProgram(const tpch::text::Text& text, RandomDevice& random_device,
                                    const int32_t scale_factor) {
  Program program;

  program.AddAssign(Assignment("orderkey_notsparse", std::make_shared<UniqueIntegerGenerator<arrow::Int32Type>>()));
  program.AddAssign(
      Assignment(OrdersTable::kOrderkey, std::make_shared<tpch::SparseKeyGenerator>("orderkey_notsparse", 3, 2)));

  program.AddAssign(Assignment(OrdersTable::kCustkey, std::make_shared<tpch::orders::CustomerkeyGenerator>(
                                                          1, scale_factor * 150'000, random_device)));

  program.AddAssign(Assignment(OrdersTable::kOrderdate, std::make_shared<UniformIntegerGenerator<arrow::Date32Type>>(
                                                            kStartDate, kEndDate - 151, random_device)));

  auto priorities_list = tpch::GetPrioritiesList();
  program.AddAssign(Assignment(OrdersTable::kOrderpriority,
                               std::make_shared<StringFromListGenerator>(*priorities_list, random_device)));

  program.AddAssign(Assignment(
      "clerk_id", std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(1, scale_factor * 1'000, random_device)));
  program.AddAssign(Assignment("clerk_id_string", std::make_shared<ToStringGenerator<arrow::Int32Type>>("clerk_id")));
  program.AddAssign(
      Assignment("clerk_id_string_zeros", std::make_shared<tpch::AppendLeadingZerosGenerator>("clerk_id_string", 9)));
  program.AddAssign(Assignment("clerk_name_prefix", std::make_shared<ConstantGenerator<arrow::StringType>>("Clerk#")));
  program.AddAssign(
      Assignment(OrdersTable::kClerk, std::make_shared<ConcatenateGenerator>(
                                          std::vector<std::string>{"clerk_name_prefix", "clerk_id_string_zeros"}, "")));
  program.AddProjection(Projection({OrdersTable::kOrderkey, OrdersTable::kCustkey, OrdersTable::kOrderdate,
                                    OrdersTable::kOrderpriority, OrdersTable::kClerk}));

  program.AddAssign(Assignment(OrdersTable::kShippriority, std::make_shared<ConstantGenerator<arrow::Int32Type>>(0)));

  program.AddAssign(
      Assignment(OrdersTable::kComment, std::make_shared<tpch::TextStringGenerator>(text, random_device, 19, 78)));

  program.AddAssign(
      Assignment("l_vec_size", std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(1, 7, random_device)));
  program.AddAssign(Assignment("l_repetition_levels", std::make_shared<RepetitionLevelsGenerator>("l_vec_size")));
  program.AddAssign(Assignment(LineitemTable::kOrderkey, std::make_shared<CopyGenerator<arrow::Int32Type>>(
                                                             "l_repetition_levels", OrdersTable::kOrderkey)));

  program.AddAssign(Assignment(LineitemTable::kPartkey, std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(
                                                            1, scale_factor * 200'000, random_device)));

  program.AddAssign(Assignment("corresponding_supplier",
                               std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(0, 3, random_device)));
  program.AddAssign(
      Assignment(LineitemTable::kSuppkey, std::make_shared<tpch::partsupp::SuppkeyGenerator>(
                                              LineitemTable::kPartkey, "corresponding_supplier", scale_factor)));

  program.AddAssign(
      Assignment(LineitemTable::kLinenumber, std::make_shared<PositionWithinArrayGenerator>("l_repetition_levels", 1)));

  program.AddAssign(
      Assignment("quantity_int", std::make_shared<UniformIntegerGenerator<arrow::Int64Type>>(1, 50, random_device)));
  program.AddAssign(Assignment("100", std::make_shared<ConstantGenerator<arrow::Int32Type>>(100)));
  program.AddAssign(Assignment(
      "quantity_int_scaled", std::make_shared<MultiplyGenerator<arrow::Int64Type, arrow::Int64Type, arrow::Int32Type>>(
                                 "quantity_int", "100")));
  program.AddAssign(Assignment(LineitemTable::kQuantity,
                               std::make_shared<ToDecimalGenerator<arrow::Int64Type>>("quantity_int_scaled", 10, 2)));

  program.AddAssign(
      Assignment("l_retailprice", std::make_shared<tpch::part::RetailPriceGenerator>(LineitemTable::kPartkey)));

  program.AddAssign(Assignment(
      "extendedprice_int", std::make_shared<MultiplyGenerator<arrow::Int64Type, arrow::Int64Type, arrow::Int64Type>>(
                               "quantity_int", "l_retailprice")));
  program.AddAssign(Assignment(LineitemTable::kExtendedprice,
                               std::make_shared<ToDecimalGenerator<arrow::Int64Type>>("extendedprice_int", 10, 2)));

  program.AddAssign(
      Assignment("discount_int", std::make_shared<UniformIntegerGenerator<arrow::Int64Type>>(0, 10, random_device)));
  program.AddAssign(Assignment(LineitemTable::kDiscount,
                               std::make_shared<ToDecimalGenerator<arrow::Int64Type>>("discount_int", 10, 2)));

  program.AddAssign(
      Assignment("tax_int", std::make_shared<UniformIntegerGenerator<arrow::Int64Type>>(0, 8, random_device)));
  program.AddAssign(
      Assignment(LineitemTable::kTax, std::make_shared<ToDecimalGenerator<arrow::Int64Type>>("tax_int", 10, 2)));

  program.AddAssign(Assignment("tmp_orderdate", std::make_shared<CopyGenerator<arrow::Date32Type>>(
                                                    "l_repetition_levels", OrdersTable::kOrderdate)));
  program.AddAssign(
      Assignment("shiptime", std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(1, 121, random_device)));
  program.AddAssign(Assignment(LineitemTable::kShipdate,
                               std::make_shared<AddGenerator<arrow::Date32Type, arrow::Date32Type, arrow::Int32Type>>(
                                   "tmp_orderdate", "shiptime")));

  program.AddAssign(
      Assignment("committime", std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(30, 90, random_device)));
  program.AddAssign(Assignment(LineitemTable::kCommitdate,
                               std::make_shared<AddGenerator<arrow::Date32Type, arrow::Date32Type, arrow::Int32Type>>(
                                   "tmp_orderdate", "committime")));

  program.AddAssign(
      Assignment("receipttime", std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(1, 30, random_device)));
  program.AddAssign(Assignment(LineitemTable::kReceiptdate,
                               std::make_shared<AddGenerator<arrow::Date32Type, arrow::Date32Type, arrow::Int32Type>>(
                                   LineitemTable::kShipdate, "receipttime")));

  auto shipinstruct_list = tpch::GetInstructionsList();
  program.AddAssign(Assignment(LineitemTable::kShipinstruct,
                               std::make_shared<StringFromListGenerator>(*shipinstruct_list, random_device)));

  auto shipmode_list = tpch::GetModesList();
  program.AddAssign(
      Assignment(LineitemTable::kShipmode, std::make_shared<StringFromListGenerator>(*shipmode_list, random_device)));

  program.AddAssign(
      Assignment(LineitemTable::kComment, std::make_shared<tpch::TextStringGenerator>(text, random_device, 10, 43)));

  program.AddAssign(Assignment(
      LineitemTable::kReturnflag,
      std::make_shared<tpch::lineitem::ReturnflagGenerator>(LineitemTable::kReceiptdate, kCurrentDate, random_device)));

  program.AddAssign(Assignment(LineitemTable::kLinestatus, std::make_shared<tpch::lineitem::LinestatusGenerator>(
                                                               LineitemTable::kShipdate, kCurrentDate)));

  program.AddAssign(Assignment(OrdersTable::kOrderstatus, std::make_shared<tpch::orders::OrderstatusGenerator>(
                                                              "l_repetition_levels", LineitemTable::kLinestatus)));

  program.AddAssign(
      Assignment("totalprice_int", std::make_shared<tpch::orders::TotalpriceGenerator>(
                                       "l_repetition_levels", "extendedprice_int", "tax_int", "discount_int")));

  program.AddAssign(Assignment(OrdersTable::kTotalprice,
                               std::make_shared<ToDecimalGenerator<arrow::Int64Type>>("totalprice_int", 10, 2)));

  std::vector<std::string> all_columns = OrdersTable().MakeColumnNames();
  std::vector<std::string> lineitem_columns = LineitemTable().MakeColumnNames();
  all_columns.insert(all_columns.end(), lineitem_columns.begin(), lineitem_columns.end());
  program.AddProjection(Projection(all_columns));

  return program;
}

struct NationTable : public Table {
  static constexpr std::string_view kNationKey = "n_nationkey";
  static constexpr std::string_view kName = "n_name";
  static constexpr std::string_view kRegionKey = "n_regionkey";
  static constexpr std::string_view kComment = "n_comment";

  std::shared_ptr<arrow::Schema> MakeArrowSchema() const override {
    arrow::FieldVector fields;
    fields.emplace_back(arrow::field(std::string(kNationKey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kName), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kRegionKey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kComment), arrow::utf8()));
    return std::make_shared<arrow::Schema>(fields);
  }

  std::string Name() const override { return "nation"; }
};

Program MakeNationProgram(const tpch::text::Text& text, RandomDevice& random_device) {
  Program program;

  std::vector<int32_t> nation_keys(25);
  std::iota(nation_keys.begin(), nation_keys.end(), 0);
  std::vector<std::string> nation_names = {"ALGERIA",      "ARGENTINA",  "BRAZIL",  "CANADA",         "EGYPT",
                                           "ETHIOPIA",     "FRANCE",     "GERMANY", "INDIA",          "INDONESIA",
                                           "IRAN",         "IRAQ",       "JAPAN",   "JORDAN",         "KENYA",
                                           "MOROCCO",      "MOZAMBIQUE", "PERU",    "CHINA",          "ROMANIA",
                                           "SAUDI ARABIA", "VIETNAM",    "RUSSIA",  "UNITED KINGDOM", "UNITED STATES"};
  std::vector<int32_t> region_keys = {0, 1, 1, 1, 4, 0, 3, 3, 2, 2, 4, 4, 2, 4, 0, 0, 0, 1, 2, 3, 4, 2, 3, 3, 1};
  program.AddAssign(
      Assignment(NationTable::kNationKey, std::make_shared<FromArrayGenerator<arrow::Int32Type>>(nation_keys)));
  program.AddAssign(
      Assignment(NationTable::kName, std::make_shared<FromArrayGenerator<arrow::StringType>>(nation_names)));
  program.AddAssign(
      Assignment(NationTable::kRegionKey, std::make_shared<FromArrayGenerator<arrow::Int32Type>>(region_keys)));
  program.AddAssign(
      Assignment(NationTable::kComment, std::make_shared<tpch::TextStringGenerator>(text, random_device, 31, 114)));

  return program;
}

struct RegionTable : public Table {
  static constexpr std::string_view kRegionKey = "r_regionkey";
  static constexpr std::string_view kName = "r_name";
  static constexpr std::string_view kComment = "r_comment";

  std::shared_ptr<arrow::Schema> MakeArrowSchema() const override {
    arrow::FieldVector fields;
    fields.emplace_back(arrow::field(std::string(kRegionKey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kName), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kComment), arrow::utf8()));
    return std::make_shared<arrow::Schema>(fields);
  }

  std::string Name() const override { return "region"; }
};

Program MakeRegionProgram(const tpch::text::Text& text, RandomDevice& random_device) {
  Program program;

  std::vector<int32_t> region_keys = {0, 1, 2, 3, 4};
  std::vector<std::string> region_names = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};
  program.AddAssign(
      Assignment(RegionTable::kRegionKey, std::make_shared<FromArrayGenerator<arrow::Int32Type>>(region_keys)));
  program.AddAssign(
      Assignment(RegionTable::kName, std::make_shared<FromArrayGenerator<arrow::StringType>>(region_names)));
  program.AddAssign(
      Assignment(RegionTable::kComment, std::make_shared<tpch::TextStringGenerator>(text, random_device, 31, 115)));

  return program;
}

arrow::csv::WriteOptions TpchCsvWriteOptions() {
  arrow::csv::WriteOptions options;
  options.include_header = false;
  options.delimiter = '|';
  options.quoting_style = arrow::csv::QuotingStyle::None;
  return options;
}

struct WriteFlags {
  std::string output_dir;
  bool write_parquet;
  bool write_csv;

  friend std::ostream& operator<<(std::ostream& os, const WriteFlags& flags) {
    return os << "output_dir: " << flags.output_dir << ", write_parquet: " << flags.write_parquet
              << ", write_csv: " << flags.write_csv;
  }
};

struct GenerateFlags {
  int32_t scale_factor;
  int32_t arrow_batch_size;
  int64_t seed;

  friend std::ostream& operator<<(std::ostream& os, const GenerateFlags& flags) {
    return os << "scale_factor: " << flags.scale_factor << ", arrow_batch_size: " << flags.arrow_batch_size
              << ", seed: " << flags.seed;
  }
};

struct TableGeneratorInfo {
  Program program;
  std::vector<std::shared_ptr<Table>> tables;
  uint64_t rows;

  TableGeneratorInfo(const Program& program, const std::vector<std::shared_ptr<Table>>& tables, uint64_t rows)
      : program(program), tables(tables), rows(rows) {}
};

arrow::Status Main(const WriteFlags& write_flags, const GenerateFlags& generate_flags) {
  RandomDevice random_device(generate_flags.seed);

  tpch::text::Text text = tpch::text::GenerateText(random_device);

  const int32_t kBatchSize = generate_flags.arrow_batch_size;
  const int32_t kScaleFactor = generate_flags.scale_factor;

  constexpr int64_t kSupplierRowsPerScaleFactor = 10'000;
  constexpr int64_t kPartRowsPerScaleFactor = 200'000;
  constexpr int64_t kCustomerRowsPerScaleFactor = 150'000;
  constexpr int64_t kOrdersRowsPerScaleFactor = 1'500'000;

  const int64_t kSupplierRows = kScaleFactor * kSupplierRowsPerScaleFactor;
  const int64_t kPartRows = kScaleFactor * kPartRowsPerScaleFactor;
  const int64_t kCustomerRows = kScaleFactor * kCustomerRowsPerScaleFactor;
  const int64_t kOrdersRows = kScaleFactor * kOrdersRowsPerScaleFactor;
  constexpr int64_t kNationRows = 25;
  constexpr int64_t kRegionRows = 5;

  auto csv_writer_options = TpchCsvWriteOptions();

  std::vector<TableGeneratorInfo> table_generator_infos;
  table_generator_infos.emplace_back(MakeSupplierProgram(text, random_device, kScaleFactor),
                                     std::vector<std::shared_ptr<Table>>{std::make_shared<SupplierTable>()},
                                     kSupplierRows);
  table_generator_infos.emplace_back(
      MakePartAndPartsuppProgram(text, random_device, kScaleFactor),
      std::vector<std::shared_ptr<Table>>{std::make_shared<PartTable>(), std::make_shared<PartsuppTable>()}, kPartRows);
  table_generator_infos.emplace_back(MakeCustomerProgram(text, random_device, kScaleFactor),
                                     std::vector<std::shared_ptr<Table>>{std::make_shared<CustomerTable>()},
                                     kCustomerRows);
  table_generator_infos.emplace_back(
      MakeOrderAndLineitemProgram(text, random_device, kScaleFactor),
      std::vector<std::shared_ptr<Table>>{std::make_shared<OrdersTable>(), std::make_shared<LineitemTable>()},
      kOrdersRows);
  table_generator_infos.emplace_back(MakeNationProgram(text, random_device),
                                     std::vector<std::shared_ptr<Table>>{std::make_shared<NationTable>()}, kNationRows);
  table_generator_infos.emplace_back(MakeRegionProgram(text, random_device),
                                     std::vector<std::shared_ptr<Table>>{std::make_shared<RegionTable>()}, kRegionRows);

  for (auto& info : table_generator_infos) {
    Program& program = info.program;
    const std::vector<std::shared_ptr<Table>>& tables = info.tables;
    uint64_t rows = info.rows;

    std::vector<std::vector<std::shared_ptr<Writer>>> writers_per_table;
    for (auto table : tables) {
      std::vector<std::shared_ptr<Writer>> writers_for_table;
      if (write_flags.write_parquet) {
        writers_for_table.emplace_back(table->GetParquetWriter(write_flags.output_dir));
      }
      if (write_flags.write_csv) {
        writers_for_table.emplace_back(table->GetCSVWriter(write_flags.output_dir, csv_writer_options));
      }
      writers_per_table.emplace_back(std::move(writers_for_table));
    }

    BatchSizeMaker batch_size_maker(kBatchSize, rows);

    for (int64_t rows_in_next_batch = batch_size_maker.NextBatchSize(); rows_in_next_batch != 0;
         rows_in_next_batch = batch_size_maker.NextBatchSize()) {
      ARROW_ASSIGN_OR_RAISE(auto batch, program.Generate(rows_in_next_batch));
      for (size_t table_num = 0; table_num < tables.size(); table_num++) {
        auto table = tables[table_num];
        auto column_names = table->MakeColumnNames();
        ARROW_ASSIGN_OR_RAISE(auto arrow_batch, batch->GetArrowBatch(column_names));
        for (auto writer : writers_per_table[table_num]) {
          ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(arrow_batch));
        }
      }
    }
  }

  return arrow::Status::OK();
}

ABSL_FLAG(std::string, output_dir, "", "output directory");
ABSL_FLAG(int64_t, seed, 0, "seed for random device");
ABSL_FLAG(int32_t, arrow_batch_size, 8192, "arrow batch size (rows)");
ABSL_FLAG(int32_t, scale_factor, 1, "scale factor");
ABSL_FLAG(bool, write_parquet, false, "write parquet files");
ABSL_FLAG(bool, write_csv, false, "write csv files");

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  std::string output_dir = absl::GetFlag(FLAGS_output_dir);
  int64_t seed = absl::GetFlag(FLAGS_seed);
  int32_t arrow_batch_size = absl::GetFlag(FLAGS_arrow_batch_size);
  int32_t scale_factor = absl::GetFlag(FLAGS_scale_factor);
  bool write_parquet = absl::GetFlag(FLAGS_write_parquet);
  bool write_csv = absl::GetFlag(FLAGS_write_csv);

  if (output_dir.empty()) {
    std::cerr << "output_dir must be set" << std::endl;
    return 1;
  }

  if (!write_parquet && !write_csv) {
    std::cerr << "Nothing to do. Please set --write_parquet or/and --write_csv" << std::endl;
    return 1;
  }

  WriteFlags write_flags{.output_dir = output_dir, .write_parquet = write_parquet, .write_csv = write_csv};
  GenerateFlags generate_flags{.scale_factor = scale_factor, .arrow_batch_size = arrow_batch_size, .seed = seed};

  std::cerr << "write_flags: " << write_flags << std::endl;
  std::cerr << "generate_flags: " << generate_flags << std::endl;

  try {
    auto status = Main(write_flags, generate_flags);

    if (!status.ok()) {
      std::cerr << status.message() << std::endl;
      return 1;
    }
  } catch (const std::exception& e) {
    std::cerr << e.what() << std::endl;
  } catch (const arrow::Status& status) {
    std::cerr << status.message() << std::endl;
  }

  return 0;
}
