#include "iceberg/tea_rest_catalog.h"

#include <stdexcept>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "cpr/api.h"
#include "iceberg/tea_remote_catalog.h"
#include "iceberg/tea_scan.h"

namespace iceberg::ice_tea {

RESTClientImpl::RESTClientImpl(const std::string& host, int port)
    : base_url_("http://" + host + ":" + std::to_string(port) + "/api/v2/trees/main/") {}

std::optional<rapidjson::Document> RESTClientImpl::GetTable(const std::string& db_name, const std::string& table_name) {
  std::string url = base_url_ + "contents/" + table_name;

  auto response = cpr::Get(cpr::Url{url});

  if (response.status_code != 200) {
    return std::nullopt;
  }

  rapidjson::Document doc;
  doc.Parse(response.text.c_str());

  if (doc.HasParseError()) {
    throw std::runtime_error("Failed to parse JSON response for table: " + table_name);
  }

  return doc;
}

std::string RESTClientImpl::GetMetadataLocation(const std::string& db_name, const std::string& table_name) {
  rapidjson::Document table_doc = GetTable(db_name, table_name).value();
  if (!table_doc.IsObject() || !table_doc.HasMember("content")) {
    throw std::runtime_error("Incorrect table representation in JSON - should have content field");
  }
  auto content = table_doc["content"].GetObject();

  if (content.HasMember("metadataLocation")) {
    std::string res = content["metadataLocation"].GetString();
    return res;
  }
  throw std::runtime_error("Table '" + table_name + "' has no metadata_location");
}

bool RESTClientImpl::TableExists(const std::string& db_name, const std::string& table_name) {
  auto maybe_table_doc = GetTable(db_name, table_name);
  if (!maybe_table_doc.has_value()) {
    return false;
  }

  auto table_doc = std::move(*maybe_table_doc);
  if (table_doc.IsObject() && table_doc.HasMember("content")) {
    return true;
  }
  return false;
}

RESTCatalog::RESTCatalog(const std::string& host, int port, std::shared_ptr<arrow::fs::S3FileSystem> s3fs)
    : RemoteCatalog(std::make_unique<RESTClientImpl>(host, port), s3fs) {}

}  // namespace iceberg::ice_tea
