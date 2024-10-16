#include "gen/tpch/tables.h"

#include <string_view>
#include <vector>

#include "gen/src/program.h"
#include "gen/src/table.h"
#include "gen/tpch/generators.h"
#include "gen/tpch/list.h"
#include "gen/tpch/text.h"

namespace gen {

constexpr int32_t kStartDate = 8035;
constexpr int32_t kCurrentDate = 9298;
constexpr int32_t kEndDate = 10591;

std::shared_ptr<arrow::Schema> SupplierTable::MakeArrowSchema() const {
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
                               std::make_shared<tpch::PhoneGenerator>(SupplierTable::kNationkey, random_device)));

  program.AddAssign(Assignment(
      "acctbal_int", std::make_shared<UniformIntegerGenerator<arrow::Int64Type>>(-99'999, 999'999, random_device)));

  program.AddAssign(Assignment(SupplierTable::kAcctbal,
                               std::make_shared<ToDecimalGenerator<arrow::Int64Type>>("acctbal_int", 10, 2)));

  program.AddAssign(
      Assignment(SupplierTable::kComment, std::make_shared<tpch::supplier::CommentGenerator>(text, random_device)));

  program.AddProjection(Projection(SupplierTable().MakeColumnNames()));

  return program;
}

std::shared_ptr<arrow::Schema> PartTable::MakeArrowSchema() const {
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

std::shared_ptr<arrow::Schema> PartsuppTable::MakeArrowSchema() const {
  arrow::FieldVector fields;
  fields.emplace_back(arrow::field(std::string(kPartkey), arrow::int32()));
  fields.emplace_back(arrow::field(std::string(kSuppkey), arrow::int32()));
  fields.emplace_back(arrow::field(std::string(kAvailqty), arrow::int32()));
  fields.emplace_back(arrow::field(std::string(kSupplycost), arrow::decimal128(10, 2)));
  fields.emplace_back(arrow::field(std::string(kComment), arrow::utf8()));
  return std::make_shared<arrow::Schema>(fields);
}

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

std::shared_ptr<arrow::Schema> CustomerTable::MakeArrowSchema() const {
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

std::shared_ptr<arrow::Schema> OrdersTable::MakeArrowSchema() const {
  arrow::FieldVector fields;
  fields.emplace_back(arrow::field(std::string(kOrderkey), arrow::int64()));
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

std::shared_ptr<arrow::Schema> LineitemTable::MakeArrowSchema() const {
  arrow::FieldVector fields;
  fields.emplace_back(arrow::field(std::string(kOrderkey), arrow::int64()));
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
  program.AddAssign(Assignment(LineitemTable::kOrderkey, std::make_shared<CopyGenerator<arrow::Int64Type>>(
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

std::shared_ptr<arrow::Schema> NationTable::MakeArrowSchema() const {
  arrow::FieldVector fields;
  fields.emplace_back(arrow::field(std::string(kNationKey), arrow::int32()));
  fields.emplace_back(arrow::field(std::string(kName), arrow::utf8()));
  fields.emplace_back(arrow::field(std::string(kRegionKey), arrow::int32()));
  fields.emplace_back(arrow::field(std::string(kComment), arrow::utf8()));
  return std::make_shared<arrow::Schema>(fields);
}

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

std::shared_ptr<arrow::Schema> RegionTable::MakeArrowSchema() const {
  arrow::FieldVector fields;
  fields.emplace_back(arrow::field(std::string(kRegionKey), arrow::int32()));
  fields.emplace_back(arrow::field(std::string(kName), arrow::utf8()));
  fields.emplace_back(arrow::field(std::string(kComment), arrow::utf8()));
  return std::make_shared<arrow::Schema>(fields);
}

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

}  // namespace gen
