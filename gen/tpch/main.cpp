#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <ostream>
#include <stdexcept>
#include <unordered_set>

#include "absl/flags/flag.h"
#include "absl/flags/internal/flag.h"
#include "absl/flags/parse.h"
#include "arrow/csv/options.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "arrow/type_fwd.h"
#include "gen/src/equality_delete.h"
#include "gen/src/fields.h"
#include "gen/src/generators.h"
#include "gen/src/positional_delete.h"
#include "gen/src/processor.h"
#include "gen/src/program.h"
#include "gen/src/table.h"
#include "gen/src/writer.h"
#include "gen/tpch/generators.h"
#include "gen/tpch/list.h"
#include "gen/tpch/tables.h"
#include "gen/tpch/text.h"
#include "parquet/schema.h"

namespace gen {

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
  int32_t files_per_table;
  bool use_equality_deletes;
  int32_t equality_deletes_columns_count;
  double equality_deletes_rows_scale;
  bool use_positional_deletes;
  double positional_deletes_rows_scale;

  friend std::ostream& operator<<(std::ostream& os, const GenerateFlags& flags) {
    return os << "scale_factor: " << flags.scale_factor << ", arrow_batch_size: " << flags.arrow_batch_size
              << ", seed: " << flags.seed << ", files_per_table: " << flags.files_per_table
              << ", use_equality_deletes: " << flags.use_equality_deletes
              << ", equality_deletes_columns_count: " << flags.equality_deletes_columns_count
              << ", equality_deletes_rows_scale: " << flags.equality_deletes_rows_scale
              << ", use_positional_deletes: " << flags.use_positional_deletes
              << ", positional_deletes_rows_scale: " << flags.positional_deletes_rows_scale;
  }
};

struct TableGeneratorInfo {
  Program program;
  std::vector<std::shared_ptr<Table>> tables;
  uint64_t rows;

  TableGeneratorInfo(const Program& program, const std::vector<std::shared_ptr<Table>>& tables, uint64_t rows)
      : program(program), tables(tables), rows(rows) {}
};

arrow::Result<std::shared_ptr<IProcessor>> MakeEqualityDeleteProcessor(std::shared_ptr<Table> table, int file_index,
                                                                       RandomDevice& random_device,
                                                                       const WriteFlags& write_flags,
                                                                       const GenerateFlags& generate_flags) {
  std::vector<std::string> equality_delete_columns;
  std::ranges::sample(table->MakeColumnNames(), std::back_inserter(equality_delete_columns),
                      generate_flags.equality_deletes_columns_count, random_device);
  std::unordered_set<std::string> selected_columns_set;
  for (const auto& field : equality_delete_columns) {
    selected_columns_set.insert(field);
  }

  auto generator_masks = std::make_shared<BernouliGenerator<arrow::BooleanType>>(
      generate_flags.equality_deletes_rows_scale, random_device);

  auto equality_delete_processor = std::make_shared<EqualityDeleteProcessor>(equality_delete_columns, generator_masks);

  auto filtered_parquet_schema = table->MakeArrowSchema();
  std::vector<int> indices_to_delete;
  for (int field_index = filtered_parquet_schema->num_fields() - 1; field_index >= 0; --field_index) {
    if (!selected_columns_set.contains(filtered_parquet_schema->fields()[field_index]->name())) {
      indices_to_delete.push_back(field_index);
    }
  }

  for (int index_to_delete : indices_to_delete) {
    ARROW_ASSIGN_OR_RAISE(filtered_parquet_schema, filtered_parquet_schema->RemoveField(index_to_delete));
  }

  auto equality_delete_writer = std::make_shared<ParquetWriter>(
      write_flags.output_dir + "/" + table->Name() + "_equality_delete_" + std::to_string(file_index) + ".parquet",
      ParquetSchemaFromArrowSchema(filtered_parquet_schema));

  auto diff_equality_delete_writer = std::make_shared<ParquetWriter>(
      write_flags.output_dir + "/" + table->Name() + "_diff_equality_delete_" + std::to_string(file_index) + ".parquet",
      table->MakeParquetSchema());

  equality_delete_processor->SetDeleteProcessor(std::make_shared<WriterProcessor>(equality_delete_writer));
  equality_delete_processor->SetResultDataProcessor(std::make_shared<WriterProcessor>(diff_equality_delete_writer));

  return equality_delete_processor;
}

arrow::Result<std::shared_ptr<IProcessor>> MakePositionalDeleteProcessor(std::shared_ptr<Table> table, int file_index,
                                                                         const std::string& data_file_name,
                                                                         RandomDevice& random_device,
                                                                         const WriteFlags& write_flags,
                                                                         const GenerateFlags& generate_flags) {
  BernouliDistribution positional_delete_gen(generate_flags.positional_deletes_rows_scale);
  auto positional_delete_processor = std::make_shared<PositionalDeleteProcessor<BernouliDistribution>>(
      data_file_name, std::move(positional_delete_gen), random_device);

  arrow::FieldVector positional_delete_fields;
  positional_delete_fields.emplace_back(arrow::field("file", arrow::utf8()));
  positional_delete_fields.emplace_back(arrow::field("row", arrow::int32()));
  auto positional_delete_schema = std::make_shared<arrow::Schema>(positional_delete_fields);

  auto positional_delete_writer = std::make_shared<ParquetWriter>(
      write_flags.output_dir + "/" + table->Name() + "_positional_delete_" + std::to_string(file_index) + ".parquet",
      ParquetSchemaFromArrowSchema(positional_delete_schema));

  auto diff_positional_delete_writer =
      std::make_shared<ParquetWriter>(write_flags.output_dir + "/" + table->Name() + "_diff_positional_delete_" +
                                          std::to_string(file_index) + ".parquet",
                                      table->MakeParquetSchema());

  positional_delete_processor->SetDeleteProcessor(std::make_shared<WriterProcessor>(positional_delete_writer));
  positional_delete_processor->SetResultDataProcessor(std::make_shared<WriterProcessor>(diff_positional_delete_writer));

  return positional_delete_processor;
}

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

    std::vector<std::shared_ptr<IProcessor>> processor_roots;
    for (auto table : tables) {
      auto processor_root = std::make_shared<AllProcessor>();
      if (write_flags.write_parquet) {
        std::vector<std::shared_ptr<IProcessor>> writers;

        for (int32_t i = 0; i < generate_flags.files_per_table; ++i) {
          auto data_file_name = write_flags.output_dir + "/" + table->Name() + std::to_string(i) + ".parquet";
          auto writer = std::make_shared<ParquetWriter>(data_file_name, table->MakeParquetSchema());
          writers.emplace_back(std::make_shared<WriterProcessor>(writer));

          if (generate_flags.use_equality_deletes && generate_flags.equality_deletes_columns_count > 0) {
            ARROW_ASSIGN_OR_RAISE(auto equality_delete_processor,
                                  MakeEqualityDeleteProcessor(table, i, random_device, write_flags, generate_flags));

            processor_root->AttachChild(equality_delete_processor);
          }

          if (generate_flags.use_positional_deletes) {
            ARROW_ASSIGN_OR_RAISE(
                auto positional_delete_processor,
                MakePositionalDeleteProcessor(table, i, data_file_name, random_device, write_flags, generate_flags));
            processor_root->AttachChild(positional_delete_processor);
          }
        }
        processor_root->AttachChild(std::make_shared<RoundRobinProcessor>(writers));
      }
#ifdef HAS_ARROW_CSV
      if (write_flags.write_csv) {
        std::vector<std::shared_ptr<IProcessor>> writers;
        for (int32_t i = 0; i < generate_flags.files_per_table; ++i) {
          auto writer =
              std::make_shared<CSVWriter>(write_flags.output_dir + "/" + table->Name() + std::to_string(i) + ".csv",
                                          table->MakeArrowSchema(), TpchCsvWriteOptions());
          writers.emplace_back(std::make_shared<WriterProcessor>(writer));
        }
        processor_root->AttachChild(std::make_shared<RoundRobinProcessor>(writers));
      }
#endif
      processor_roots.push_back(processor_root);
    }

    BatchSizeMaker batch_size_maker(kBatchSize, rows);

    for (int64_t rows_in_next_batch = batch_size_maker.NextBatchSize(); rows_in_next_batch != 0;
         rows_in_next_batch = batch_size_maker.NextBatchSize()) {
      ARROW_ASSIGN_OR_RAISE(auto batch, program.Generate(rows_in_next_batch));
      for (size_t table_num = 0; table_num < tables.size(); table_num++) {
        auto table = tables[table_num];
        auto column_names = table->MakeColumnNames();
        ARROW_ASSIGN_OR_RAISE(auto proj, batch->GetProjection(column_names));
        ARROW_ASSIGN_OR_RAISE(auto arrow_batch, batch->GetArrowBatch(column_names));
        ARROW_RETURN_NOT_OK(processor_roots[table_num]->Process(proj));
      }
    }
  }

  return arrow::Status::OK();
}

}  // namespace gen

ABSL_FLAG(std::string, output_dir, "", "output directory");
ABSL_FLAG(int64_t, seed, 0, "seed for random device");
ABSL_FLAG(int32_t, arrow_batch_size, 8192, "arrow batch size (rows)");
ABSL_FLAG(int32_t, scale_factor, 1, "scale factor");
ABSL_FLAG(int32_t, files_per_table, 1, "files_per_table");
ABSL_FLAG(bool, write_parquet, false, "write parquet files");
ABSL_FLAG(bool, write_csv, false, "write csv files");
ABSL_FLAG(bool, use_equality_deletes, false, "either to generate equality delete files to all tables or not");
ABSL_FLAG(int32_t, equality_deletes_columns_count, 0, "number of columns in equality deletes files");
ABSL_FLAG(double, equality_deletes_rows_scale, 0, "row count ratio in equality delete files and in original table");
ABSL_FLAG(bool, use_positional_deletes, false, "either to generate positional delete files to all tables or not");
ABSL_FLAG(double, positional_deletes_rows_scale, 0, "row count ratio in equality delete files and in original table");

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  std::string output_dir = absl::GetFlag(FLAGS_output_dir);
  int64_t seed = absl::GetFlag(FLAGS_seed);
  int32_t arrow_batch_size = absl::GetFlag(FLAGS_arrow_batch_size);
  int32_t scale_factor = absl::GetFlag(FLAGS_scale_factor);
  int32_t files_per_table = absl::GetFlag(FLAGS_files_per_table);
  bool write_parquet = absl::GetFlag(FLAGS_write_parquet);
  bool write_csv = absl::GetFlag(FLAGS_write_csv);

  if (write_csv) {
#ifndef HAS_ARROW_CSV
    std::cout << "arrow built without csv" << std::endl;
    return 1;
#endif
  }

  bool use_equality_deletes = absl::GetFlag(FLAGS_use_equality_deletes);
  int32_t equality_deletes_columns_count = absl::GetFlag(FLAGS_equality_deletes_columns_count);
  double equality_deletes_rows_scale = absl::GetFlag(FLAGS_equality_deletes_rows_scale);

  bool use_positional_deletes = absl::GetFlag(FLAGS_use_positional_deletes);
  double positional_deletes_rows_scale = absl::GetFlag(FLAGS_positional_deletes_rows_scale);

  if (output_dir.empty()) {
    std::cerr << "output_dir must be set" << std::endl;
    return 1;
  }

  if (!write_parquet && !write_csv) {
    std::cerr << "Nothing to do. Please set --write_parquet or/and --write_csv" << std::endl;
    return 1;
  }

  gen::WriteFlags write_flags{.output_dir = output_dir, .write_parquet = write_parquet, .write_csv = write_csv};
  gen::GenerateFlags generate_flags{.scale_factor = scale_factor,
                                    .arrow_batch_size = arrow_batch_size,
                                    .seed = seed,
                                    .files_per_table = files_per_table,
                                    .use_equality_deletes = use_equality_deletes,
                                    .equality_deletes_columns_count = equality_deletes_columns_count,
                                    .equality_deletes_rows_scale = equality_deletes_rows_scale,
                                    .use_positional_deletes = use_positional_deletes,
                                    .positional_deletes_rows_scale = positional_deletes_rows_scale};

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
