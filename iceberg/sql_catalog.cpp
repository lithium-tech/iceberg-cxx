#include "iceberg/sql_catalog.h"

#include <memory>
#include <stdexcept>

#include "iceberg/table.h"

namespace iceberg {

SqlCatalog::SqlCatalog(const std::string& root_dir, std::shared_ptr<arrow::fs::FileSystem> fs)
    : root_dir_(root_dir), fs_(fs) {}

std::shared_ptr<Table> SqlCatalog::LoadTable(const catalog::TableIdentifier& identifier) {
  std::vector<std::string> locations;

  arrow::fs::FileSelector selector;
  selector.base_dir = root_dir_ + "/" + identifier.db + "/" + identifier.name;
  selector.recursive = true;

  arrow::Result<std::vector<arrow::fs::FileInfo>> result = fs_->GetFileInfo(selector);
  if (result.ok()) {
    for (const auto& file_info : result.ValueOrDie()) {
      locations.push_back(file_info.path());
    }
  } else {
    throw std::runtime_error("error to listing files");
  }
  return std::make_shared<SqlTable>(identifier, selector.base_dir, locations, fs_);
}

bool SqlCatalog::TableExists(const catalog::TableIdentifier& identifier) {
  arrow::fs::FileSelector selector;
  selector.base_dir = root_dir_ + "/" + identifier.db + "/" + identifier.name;
  selector.recursive = true;

  arrow::Result<std::vector<arrow::fs::FileInfo>> result = fs_->GetFileInfo(selector);
  if (!result.ok()) {
    return false;
  }
  return true;
}

std::shared_ptr<Table> SqlCatalog::CreateTable(const catalog::TableIdentifier& identifier, const Schema& schema,
                                               std::shared_ptr<TableMetadataV2> table_metadata) {
  auto dir_path = root_dir_ + "/" + identifier.db + "/" + identifier.name;
  auto status = fs_->CreateDir(dir_path);
  if (!status.ok()) {
    throw std::runtime_error("Could not create dir " + dir_path);
  }
  return std::make_shared<SqlTable>(identifier, dir_path, std::vector<std::string>{}, table_metadata, fs_);
}

}  // namespace iceberg
