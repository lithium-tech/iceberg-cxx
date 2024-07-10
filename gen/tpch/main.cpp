#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <stdexcept>

#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "gen/src/fields.h"
#include "gen/src/generators.h"
#include "gen/src/program.h"
#include "gen/src/writer.h"
#include "gen/tpch/generators.h"
#include "gen/tpch/list.h"
#include "gen/tpch/text.h"
#include "parquet/schema.h"

using namespace gen;

constexpr int32_t kStartDate = 8206;
constexpr int32_t kCurrentDate = 9298;
constexpr int32_t kEndDate = 10591;

struct Table {
  std::shared_ptr<parquet::schema::GroupNode> MakeParquetSchema() const {
    return ParquetSchemaFromArrowSchema(MakeArrowSchema());
  }

  virtual std::shared_ptr<const arrow::Schema> MakeArrowSchema() const = 0;

  std::vector<std::string> MakeColumnNames() const {
    std::vector<std::string> result;
    auto schema = MakeArrowSchema();
    for (const auto& field : schema->fields()) {
      result.emplace_back(field->name());
    }
    return result;
  }
};

struct SupplierTable : public Table {
  static constexpr std::string_view kSuppkey = "s_suppkey";
  static constexpr std::string_view kName = "s_name";
  static constexpr std::string_view kAddress = "s_address";
  static constexpr std::string_view kNationkey = "s_nationkey";
  static constexpr std::string_view kPhone = "s_phone";
  static constexpr std::string_view kAcctbal = "s_acctbal";
  static constexpr std::string_view kComment = "s_comment";

  std::shared_ptr<const arrow::Schema> MakeArrowSchema() const override {
    arrow::FieldVector fields;
    fields.emplace_back(arrow::field(std::string(kSuppkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kName), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kAddress), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kNationkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kPhone), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kAcctbal), arrow::decimal128(7, 2)));
    fields.emplace_back(arrow::field(std::string(kComment), arrow::utf8()));
    return std::make_shared<arrow::Schema>(fields);
  }
};

Program MakeSupplierProgram(const tpch::text::Text& text, RandomDevice& random_device, const int32_t scale_factor) {
  Program program;
  program.AddAssign(Assignment(SupplierTable::kSuppkey, std::make_shared<UniqueIntegerGenerator<arrow::Int32Type>>()));

  program.AddAssign(Assignment("s_suppkey_string", std::make_shared<ToStringGenerator<arrow::Int32Type>>(),
                               {SupplierTable::kSuppkey}));

  program.AddAssign(Assignment("s_name_prefix", std::make_shared<ConstantGenerator<arrow::StringType>>("Supplier#")));

  program.AddAssign(Assignment(SupplierTable::kName, std::make_shared<ConcatenateGenerator>(""),
                               {"s_name_prefix", "s_suppkey_string"}));

  program.AddAssign(
      Assignment(SupplierTable::kAddress, std::make_shared<tpch::VStringGenerator>(10, 40, random_device)));

  program.AddAssign(Assignment(SupplierTable::kNationkey,
                               std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(0, 24, random_device)));

  program.AddAssign(Assignment(SupplierTable::kPhone, std::make_shared<tpch::PhoneGenerator>(random_device),
                               {SupplierTable::kSuppkey}));

  program.AddAssign(Assignment(SupplierTable::kAcctbal, std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(
                                                            -99'999, 999'999, random_device)));

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

  std::shared_ptr<const arrow::Schema> MakeArrowSchema() const override {
    arrow::FieldVector fields;
    fields.emplace_back(arrow::field(std::string(kPartkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kName), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kMfgr), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kBrand), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kType), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kSize), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kContainer), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kRetailprice), arrow::decimal128(7, 2)));
    fields.emplace_back(arrow::field(std::string(kComment), arrow::utf8()));
    return std::make_shared<arrow::Schema>(fields);
  }
};

struct PartsuppTable : public Table {
  static constexpr std::string_view kPartkey = "ps_partkey";
  static constexpr std::string_view kSuppkey = "ps_suppkey";
  static constexpr std::string_view kAvailqty = "ps_availqty";
  static constexpr std::string_view kSupplycost = "ps_supplycost";
  static constexpr std::string_view kComment = "ps_comment";

  std::shared_ptr<const arrow::Schema> MakeArrowSchema() const override {
    arrow::FieldVector fields;
    fields.emplace_back(arrow::field(std::string(kPartkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kSuppkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kAvailqty), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kSupplycost), arrow::decimal128(7, 2)));
    fields.emplace_back(arrow::field(std::string(kComment), arrow::utf8()));
    return std::make_shared<arrow::Schema>(fields);
  }
};

Program MakePartAndPartsuppProgram(const tpch::text::Text& text, RandomDevice& random_device,
                                   const int32_t scale_factor) {
  Program program;

  program.AddAssign(Assignment(PartTable::kPartkey, std::make_shared<UniqueIntegerGenerator<arrow::Int32Type>>()));

  auto p_name_list = tpch::part::GetPNameList();
  auto p_name_generator = std::make_shared<tpch::part::NameGenerator>(*p_name_list, random_device);
  program.AddAssign(Assignment(PartTable::kName, p_name_generator));

  program.AddAssign(Assignment("M", std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(1, 5, random_device)));
  program.AddAssign(Assignment("M_string", std::make_shared<ToStringGenerator<arrow::Int32Type>>(), {"M"}));
  program.AddAssign(
      Assignment("Manufacturer#", std::make_shared<ConstantGenerator<arrow::StringType>>("Manufacturer#")));
  program.AddAssign(
      Assignment(PartTable::kMfgr, std::make_shared<ConcatenateGenerator>(""), {"Manufacturer#", "M_string"}));

  program.AddAssign(Assignment("N", std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(1, 5, random_device)));
  program.AddAssign(Assignment("N_string", std::make_shared<ToStringGenerator<arrow::Int32Type>>(), {"N"}));
  program.AddAssign(Assignment("Brand#", std::make_shared<ConstantGenerator<arrow::StringType>>("Brand#")));
  program.AddAssign(
      Assignment(PartTable::kBrand, std::make_shared<ConcatenateGenerator>(""), {"Brand#", "M_string", "N_string"}));
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
      Assignment(PartTable::kRetailprice, std::make_shared<tpch::part::RetailPriceGenerator>(), {PartTable::kPartkey}));

  program.AddAssign(
      Assignment(PartTable::kComment, std::make_shared<tpch::TextStringGenerator>(text, random_device, 5, 22)));

  program.AddAssign(Assignment("ps_vec_size", std::make_shared<ConstantGenerator<arrow::Int32Type>>(4)));

  program.AddAssign(Assignment("ps_repetition_levels", std::make_shared<RepetitionLevelsGenerator>(), {"ps_vec_size"}));

  program.AddAssign(Assignment(PartsuppTable::kPartkey, std::make_shared<CopyGenerator>(),
                               {"ps_repetition_levels", PartTable::kPartkey}));

  program.AddAssign(Assignment("corresponding_supplier",
                               std::make_shared<UniqueWithinArrayGenerator>(0, 3, random_device),
                               {"ps_repetition_levels"}));
  program.AddAssign(Assignment(PartsuppTable::kSuppkey,
                               std::make_shared<tpch::partsupp::SuppkeyGenerator>(scale_factor),
                               {PartsuppTable::kPartkey, "corresponding_supplier"}));

  program.AddAssign(Assignment(PartsuppTable::kAvailqty,
                               std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(1, 9999, random_device)));

  program.AddAssign(Assignment(PartsuppTable::kSupplycost, std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(
                                                               100, 100000, random_device)));

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

  std::shared_ptr<const arrow::Schema> MakeArrowSchema() const override {
    arrow::FieldVector fields;
    fields.emplace_back(arrow::field(std::string(kCustkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kName), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kAddress), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kNationkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kPhone), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kAcctbal), arrow::decimal128(7, 2)));
    fields.emplace_back(arrow::field(std::string(kMktsegment), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kComment), arrow::utf8()));
    return std::make_shared<arrow::Schema>(fields);
  }
};

Program MakeCustomerProgram(const tpch::text::Text& text, RandomDevice& random_device, const int32_t scale_factor) {
  Program program;

  program.AddAssign(Assignment(CustomerTable::kCustkey, std::make_shared<UniqueIntegerGenerator<arrow::Int32Type>>()));

  program.AddAssign(Assignment("c_custkey_string", std::make_shared<ToStringGenerator<arrow::Int32Type>>(),
                               {CustomerTable::kCustkey}));
  program.AddAssign(
      Assignment("c_custname_prefix", std::make_shared<ConstantGenerator<arrow::StringType>>("Customer#")));
  program.AddAssign(Assignment(CustomerTable::kName, std::make_shared<ConcatenateGenerator>(""),
                               {"c_custname_prefix", "c_custkey_string"}));

  program.AddAssign(
      Assignment(CustomerTable::kAddress, std::make_shared<tpch::VStringGenerator>(10, 40, random_device)));

  program.AddAssign(Assignment(CustomerTable::kNationkey,
                               std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(0, 24, random_device)));

  program.AddAssign(Assignment(CustomerTable::kPhone, std::make_shared<tpch::PhoneGenerator>(random_device),
                               {CustomerTable::kNationkey}));

  program.AddAssign(Assignment(CustomerTable::kAcctbal, std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(
                                                            -99999, 999999, random_device)));

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

  std::shared_ptr<const arrow::Schema> MakeArrowSchema() const override {
    arrow::FieldVector fields;
    fields.emplace_back(arrow::field(std::string(kOrderkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kCustkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kOrderstatus), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kTotalprice), arrow::decimal128(7, 2)));
    fields.emplace_back(arrow::field(std::string(kOrderdate), arrow::date32()));
    fields.emplace_back(arrow::field(std::string(kOrderpriority), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kClerk), arrow::utf8()));
    fields.emplace_back(arrow::field(std::string(kShippriority), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kComment), arrow::utf8()));
    return std::make_shared<arrow::Schema>(fields);
  }
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

  std::shared_ptr<const arrow::Schema> MakeArrowSchema() const override {
    arrow::FieldVector fields;
    fields.emplace_back(arrow::field(std::string(kOrderkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kPartkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kSuppkey), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kLinenumber), arrow::int32()));
    fields.emplace_back(arrow::field(std::string(kQuantity), arrow::decimal128(7, 2)));
    fields.emplace_back(arrow::field(std::string(kExtendedprice), arrow::decimal128(7, 2)));
    fields.emplace_back(arrow::field(std::string(kDiscount), arrow::decimal128(7, 2)));
    fields.emplace_back(arrow::field(std::string(kTax), arrow::decimal128(7, 2)));
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
};

Program MakeOrderAndLineitemProgram(const tpch::text::Text& text, RandomDevice& random_device,
                                    const int32_t scale_factor) {
  Program program;

  program.AddAssign(Assignment(OrdersTable::kOrderkey, std::make_shared<UniqueIntegerGenerator<arrow::Int32Type>>()));

  program.AddAssign(Assignment(OrdersTable::kCustkey, std::make_shared<tpch::orders::CustomerkeyGenerator>(
                                                          1, scale_factor * 150'000, random_device)));

  program.AddAssign(Assignment(OrdersTable::kOrderdate, std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(
                                                            kStartDate, kEndDate - 151, random_device)));

  auto priorities_list = tpch::GetPrioritiesList();
  program.AddAssign(Assignment(OrdersTable::kOrderpriority,
                               std::make_shared<StringFromListGenerator>(*priorities_list, random_device)));

  program.AddAssign(Assignment(
      "clerk_id", std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(1, scale_factor * 1'000, random_device)));
  program.AddAssign(
      Assignment("clerk_id_string", std::make_shared<ToStringGenerator<arrow::Int32Type>>(), {"clerk_id"}));
  program.AddAssign(Assignment("clerk_name_prefix", std::make_shared<ConstantGenerator<arrow::StringType>>("Clerk#")));
  program.AddAssign(Assignment(OrdersTable::kClerk, std::make_shared<ConcatenateGenerator>(""),
                               {"clerk_name_prefix", "clerk_id_string"}));
  program.AddProjection(Projection({OrdersTable::kOrderkey, OrdersTable::kCustkey, OrdersTable::kOrderdate,
                                    OrdersTable::kOrderpriority, OrdersTable::kClerk}));

  program.AddAssign(Assignment(OrdersTable::kShippriority, std::make_shared<ConstantGenerator<arrow::Int32Type>>(0)));

  program.AddAssign(
      Assignment(OrdersTable::kComment, std::make_shared<tpch::TextStringGenerator>(text, random_device, 19, 78)));

  program.AddAssign(
      Assignment("l_vec_size", std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(1, 7, random_device)));
  program.AddAssign(Assignment("l_repetition_levels", std::make_shared<RepetitionLevelsGenerator>(), {"l_vec_size"}));
  program.AddAssign(Assignment(LineitemTable::kOrderkey, std::make_shared<CopyGenerator>(),
                               {"l_repetition_levels", OrdersTable::kOrderkey}));

  program.AddAssign(Assignment(LineitemTable::kPartkey, std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(
                                                            1, scale_factor * 200'000, random_device)));

  program.AddAssign(Assignment("corresponding_supplier",
                               std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(0, 3, random_device)));
  program.AddAssign(Assignment(LineitemTable::kSuppkey,
                               std::make_shared<tpch::partsupp::SuppkeyGenerator>(scale_factor),
                               {LineitemTable::kPartkey, "corresponding_supplier"}));

  program.AddAssign(Assignment(LineitemTable::kLinenumber,
                               std::make_shared<UniqueWithinArrayGenerator>(1, 7, random_device),
                               {"l_repetition_levels"}));

  program.AddAssign(Assignment(LineitemTable::kQuantity,
                               std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(1, 50, random_device)));

  program.AddAssign(
      Assignment("l_retailprice", std::make_shared<tpch::part::RetailPriceGenerator>(), {LineitemTable::kPartkey}));

  program.AddAssign(Assignment(LineitemTable::kExtendedprice, std::make_shared<MultiplyGenerator>(),
                               {LineitemTable::kQuantity, "l_retailprice"}));

  program.AddAssign(Assignment(LineitemTable::kDiscount,
                               std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(0, 10, random_device)));

  program.AddAssign(Assignment(LineitemTable::kTax,
                               std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(0, 8, random_device)));

  program.AddAssign(
      Assignment("tmp_orderdate", std::make_shared<CopyGenerator>(), {"l_repetition_levels", OrdersTable::kOrderdate}));
  program.AddAssign(
      Assignment("shiptime", std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(1, 121, random_device)));
  program.AddAssign(
      Assignment(LineitemTable::kShipdate, std::make_shared<AddGenerator>(), {"tmp_orderdate", "shiptime"}));

  program.AddAssign(
      Assignment("committime", std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(30, 90, random_device)));
  program.AddAssign(
      Assignment(LineitemTable::kCommitdate, std::make_shared<AddGenerator>(), {"tmp_orderdate", "committime"}));

  program.AddAssign(
      Assignment("receipttime", std::make_shared<UniformIntegerGenerator<arrow::Int32Type>>(1, 30, random_device)));
  program.AddAssign(Assignment(LineitemTable::kReceiptdate, std::make_shared<AddGenerator>(),
                               {LineitemTable::kShipdate, "receipttime"}));

  auto shipinstruct_list = tpch::GetInstructionsList();
  program.AddAssign(Assignment(LineitemTable::kShipinstruct,
                               std::make_shared<StringFromListGenerator>(*shipinstruct_list, random_device)));

  auto shipmode_list = tpch::GetModesList();
  program.AddAssign(
      Assignment(LineitemTable::kShipmode, std::make_shared<StringFromListGenerator>(*shipmode_list, random_device)));

  program.AddAssign(
      Assignment(LineitemTable::kComment, std::make_shared<tpch::TextStringGenerator>(text, random_device, 10, 43)));

  program.AddAssign(Assignment(LineitemTable::kReturnflag,
                               std::make_shared<tpch::lineitem::ReturnflagGenerator>(kCurrentDate, random_device),
                               {LineitemTable::kReceiptdate}));

  program.AddAssign(Assignment(LineitemTable::kLinestatus,
                               std::make_shared<tpch::lineitem::LinestatusGenerator>(kCurrentDate),
                               {LineitemTable::kShipdate}));

  program.AddAssign(Assignment(OrdersTable::kOrderstatus, std::make_shared<tpch::orders::OrderstatusGenerator>(),
                               {"l_repetition_levels", LineitemTable::kLinestatus}));

  program.AddAssign(Assignment(
      OrdersTable::kTotalprice, std::make_shared<tpch::orders::TotalpriceGenerator>(),
      {"l_repetition_levels", LineitemTable::kExtendedprice, LineitemTable::kTax, LineitemTable::kDiscount}));

  std::vector<std::string> all_columns = OrdersTable().MakeColumnNames();
  std::vector<std::string> lineitem_columns = LineitemTable().MakeColumnNames();
  all_columns.insert(all_columns.end(), lineitem_columns.begin(), lineitem_columns.end());
  program.AddProjection(Projection(all_columns));

  return program;
}

arrow::Status Main() {
  RandomDevice random_device(2101);

  tpch::text::Text text = tpch::text::GenerateText(random_device);

  constexpr int32_t kBatchSize = 8192;
  constexpr int32_t kScaleFactor = 1;
  constexpr int64_t kSupplierRows = kScaleFactor * 10'000;
  constexpr int64_t kPartRows = kScaleFactor * 200'000;
  constexpr int64_t kCustomerRows = kScaleFactor * 150'000;
  constexpr int64_t kOrdersRows = kScaleFactor * 1'500'000;

  {
    Program supplier_program = MakeSupplierProgram(text, random_device, kScaleFactor);
    BatchSizeMaker batch_size_maker(kBatchSize, kSupplierRows);

    SupplierTable supplier_table;

    Writer writer("supplier.parquet", supplier_table.MakeParquetSchema());

    for (int64_t rows_in_next_batch = batch_size_maker.NextBatchSize(); rows_in_next_batch != 0;
         rows_in_next_batch = batch_size_maker.NextBatchSize()) {
      ARROW_ASSIGN_OR_RAISE(auto batch, supplier_program.Generate(rows_in_next_batch));
      ARROW_ASSIGN_OR_RAISE(auto arrow_batch, batch->GetArrowBatch(supplier_table.MakeColumnNames()));
      ARROW_RETURN_NOT_OK(writer.WriteRecordBatch(arrow_batch));
    }
  }

  {
    Program part_program = MakePartAndPartsuppProgram(text, random_device, kScaleFactor);
    BatchSizeMaker batch_size_maker(kBatchSize, kPartRows);

    PartTable part_table;
    PartsuppTable partsupp_table;

    Writer part_writer("part.parquet", part_table.MakeParquetSchema());
    Writer partsupp_writer("partsupp.parquet", partsupp_table.MakeParquetSchema());

    for (int64_t rows_in_next_batch = batch_size_maker.NextBatchSize(); rows_in_next_batch != 0;
         rows_in_next_batch = batch_size_maker.NextBatchSize()) {
      ARROW_ASSIGN_OR_RAISE(auto batch, part_program.Generate(rows_in_next_batch));

      ARROW_ASSIGN_OR_RAISE(auto part_arrow_batch, batch->GetArrowBatch(part_table.MakeColumnNames()));
      ARROW_RETURN_NOT_OK(part_writer.WriteRecordBatch(part_arrow_batch));

      ARROW_ASSIGN_OR_RAISE(auto partsupp_arrow_batch, batch->GetArrowBatch(partsupp_table.MakeColumnNames()));
      ARROW_RETURN_NOT_OK(partsupp_writer.WriteRecordBatch(partsupp_arrow_batch));
    }
  }

  {
    Program customer_program = MakeCustomerProgram(text, random_device, kScaleFactor);
    BatchSizeMaker batch_size_maker(kBatchSize, kCustomerRows);

    CustomerTable customer_table;

    Writer customer_writer("customer.parquet", customer_table.MakeParquetSchema());

    for (int64_t rows_in_next_batch = batch_size_maker.NextBatchSize(); rows_in_next_batch != 0;
         rows_in_next_batch = batch_size_maker.NextBatchSize()) {
      ARROW_ASSIGN_OR_RAISE(auto batch, customer_program.Generate(rows_in_next_batch));

      ARROW_ASSIGN_OR_RAISE(auto arrow_batch, batch->GetArrowBatch(customer_table.MakeColumnNames()));
      ARROW_RETURN_NOT_OK(customer_writer.WriteRecordBatch(arrow_batch));
    }
  }

  {
    Program order_and_lineitem_program = MakeOrderAndLineitemProgram(text, random_device, kScaleFactor);
    BatchSizeMaker batch_size_maker(kBatchSize, kOrdersRows);

    OrdersTable orders_table;
    LineitemTable lineitem_table;

    Writer order_writer("orders.parquet", orders_table.MakeParquetSchema());
    Writer lineitem_writer("lineitem.parquet", lineitem_table.MakeParquetSchema());

    for (int64_t rows_in_next_batch = batch_size_maker.NextBatchSize(); rows_in_next_batch != 0;
         rows_in_next_batch = batch_size_maker.NextBatchSize()) {
      ARROW_ASSIGN_OR_RAISE(auto batch, order_and_lineitem_program.Generate(rows_in_next_batch));

      ARROW_ASSIGN_OR_RAISE(auto order_arrow_batch, batch->GetArrowBatch(orders_table.MakeColumnNames()));
      ARROW_RETURN_NOT_OK(order_writer.WriteRecordBatch(order_arrow_batch));

      ARROW_ASSIGN_OR_RAISE(auto lineitem_arrow_batch, batch->GetArrowBatch(lineitem_table.MakeColumnNames()));
      ARROW_RETURN_NOT_OK(lineitem_writer.WriteRecordBatch(lineitem_arrow_batch));
    }
  }

  return arrow::Status::OK();
}

int main() {
  auto status = Main();

  if (!status.ok()) {
    std::cerr << status.message();
    return 1;
  }

  return 0;
}
