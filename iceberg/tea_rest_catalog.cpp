#include "iceberg/tea_rest_catalog.h"

#include <memory>

#include "cpr/api.h"
#include "rapidjson/document.h"

namespace iceberg::ice_tea {

class RESTClientImpl : public IMetadataClient {
 private:
  std::string base_url;

 public:
  RESTClientImpl(const std::string& rest_url, const std::string& warehouse_id)
      : base_url(rest_url + "/v1/" + warehouse_id) {}

  std::string GetMetadataLocation(const std::string& db_name, const std::string& table_name) override {
    std::string url = base_url + "/namespaces/" + db_name + "/tables/" + table_name;

    cpr::Response response = cpr::Get(cpr::Url{url});
    if (response.status_code != 200) {
      throw std::runtime_error("Error " + std::to_string(response.status_code) + " when getting JSON for table " +
                               db_name + '.' + table_name);
    }

    rapidjson::Document doc;
    doc.Parse(response.text.c_str());
    Ensure(!doc.HasParseError(), "Failed to parse JSON response for table: " + db_name + '.' + table_name);
    Ensure(doc.IsObject() && doc.HasMember("metadata-location"),
           "Incorrect table representation in JSON - should have metadata-location field");
    return doc["metadata-location"].GetString();
  }
};

RESTCatalog::RESTCatalog(const std::string& rest_url, const std::string& warehouse_id)
    : RemoteCatalog(std::make_unique<RESTClientImpl>(rest_url, warehouse_id)) {}

}  // namespace iceberg::ice_tea
