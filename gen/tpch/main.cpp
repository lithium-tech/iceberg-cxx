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
#include "gen/src/processor.h"
#include "gen/src/program.h"
#include "gen/src/table.h"
#include "gen/src/writer.h"
#include "gen/tpch/generators.h"
#include "gen/tpch/list.h"
#include "gen/tpch/tables.h"
#include "gen/tpch/text.h"
#include "parquet/schema.h"

using namespace gen;

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

  friend std::ostream& operator<<(std::ostream& os, const GenerateFlags& flags) {
    return os << "scale_factor: " << flags.scale_factor << ", arrow_batch_size: " << flags.arrow_batch_size
              << ", seed: " << flags.seed << ", files_per_table: " << flags.files_per_table;
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

    std::vector<std::vector<std::shared_ptr<IProcessor>>> writers_per_table;
    for (auto table : tables) {
      std::vector<std::shared_ptr<IProcessor>> writers_for_table;
      if (write_flags.write_parquet) {
        std::vector<std::shared_ptr<IProcessor>> writers;
        for (int32_t i = 0; i < generate_flags.files_per_table; ++i) {
          auto writer = std::make_shared<ParquetWriter>(
              write_flags.output_dir + "/" + table->Name() + std::to_string(i) + ".parquet",
              table->MakeParquetSchema());
          writers.emplace_back(std::make_shared<WriterProcessor>(writer));
        }
        writers_for_table.emplace_back(std::make_shared<RoundRobinProcessor>(writers));
      }
      if (write_flags.write_csv) {
        std::vector<std::shared_ptr<IProcessor>> writers;
        for (int32_t i = 0; i < generate_flags.files_per_table; ++i) {
          auto writer =
              std::make_shared<CSVWriter>(write_flags.output_dir + "/" + table->Name() + std::to_string(i) + ".csv",
                                          table->MakeArrowSchema(), TpchCsvWriteOptions());
          writers.emplace_back(std::make_shared<WriterProcessor>(writer));
        }
        writers_for_table.emplace_back(std::make_shared<RoundRobinProcessor>(writers));
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
        ARROW_ASSIGN_OR_RAISE(auto proj, batch->GetProjection(column_names));
        ARROW_ASSIGN_OR_RAISE(auto arrow_batch, batch->GetArrowBatch(column_names));
        for (auto writer : writers_per_table[table_num]) {
          ARROW_RETURN_NOT_OK(writer->Process(proj));
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
ABSL_FLAG(int32_t, files_per_table, 1, "files_per_table");
ABSL_FLAG(bool, write_parquet, false, "write parquet files");
ABSL_FLAG(bool, write_csv, false, "write csv files");

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  std::string output_dir = absl::GetFlag(FLAGS_output_dir);
  int64_t seed = absl::GetFlag(FLAGS_seed);
  int32_t arrow_batch_size = absl::GetFlag(FLAGS_arrow_batch_size);
  int32_t scale_factor = absl::GetFlag(FLAGS_scale_factor);
  int32_t files_per_table = absl::GetFlag(FLAGS_files_per_table);
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
  GenerateFlags generate_flags{.scale_factor = scale_factor,
                               .arrow_batch_size = arrow_batch_size,
                               .seed = seed,
                               .files_per_table = files_per_table};

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
