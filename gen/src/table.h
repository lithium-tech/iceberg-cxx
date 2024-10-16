#pragma once

#include <memory>
#include <string>
#include <vector>

#include "gen/src/fields.h"
#include "gen/src/writer.h"

namespace gen {

struct Table {
  std::shared_ptr<parquet::schema::GroupNode> MakeParquetSchema() const {
    return ParquetSchemaFromArrowSchema(MakeArrowSchema());
  }

  virtual std::shared_ptr<arrow::Schema> MakeArrowSchema() const = 0;

  virtual std::string Name() const = 0;

  std::shared_ptr<CSVWriter> GetCSVWriter(const std::string& output_dir,
                                          const arrow::csv::WriteOptions& options) const {
    return std::make_shared<CSVWriter>(output_dir + "/" + Name() + ".csv", MakeArrowSchema(), options);
  }

  std::vector<std::string> MakeColumnNames() const {
    std::vector<std::string> result;
    auto schema = MakeArrowSchema();
    for (const auto& field : schema->fields()) {
      result.emplace_back(field->name());
    }
    return result;
  }

  virtual ~Table() = default;
};

}  // namespace gen
