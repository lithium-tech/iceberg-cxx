#include "iceberg/tests/src/config.h"

#include <arrow/status.h>

#include <algorithm>
#include <chrono>
#include <cinttypes>
#include <memory>
#include <string>

#include "ini/ini.h"

namespace iceberg {

// TODO(gmusya): do not use hardcoded path
const char* Config::kFilePath = "tea/tea.cfg";

namespace {

using IniPointer = std::unique_ptr<ini_t, void (*)(ini_t*)>;

void IniGet(ini_t* ini, const char* section, const char* key,
            std::string* out) {
  if (auto value = ini_get(ini, section, key)) {
    *out = value;
  }
}

void IniGet(ini_t* ini, const char* section, const char* key,
            std::chrono::milliseconds* out) {
  int value;
  if (ini_sget(ini, section, key, "%d", &value)) {
    *out = std::chrono::milliseconds(value);
  }
}

void LoadEnvDefaults(Config* config) {
  if (auto* var = getenv("AWS_ENDPOINT_URL")) {
    config->s3.endpoint_override = std::string(var);
  }
  if (auto* var = getenv("AWS_DEFAULT_REGION")) {
    config->s3.region = std::string(var);
  }
  if (auto* var = getenv("AWS_ACCESS_KEY_ID")) {
    config->s3.access_key = std::string(var);
  }
  if (auto* var = getenv("AWS_SECRET_ACCESS_KEY")) {
    config->s3.secret_key = std::string(var);
  }
}

arrow::Status ReadValues(IniPointer ini, Config* config) {
  LoadEnvDefaults(config);

  IniGet(ini.get(), "s3", "access_key", &config->s3.access_key);
  IniGet(ini.get(), "s3", "secret_key", &config->s3.secret_key);
  IniGet(ini.get(), "s3", "endpoint_override", &config->s3.endpoint_override);
  IniGet(ini.get(), "s3", "region", &config->s3.region);
  IniGet(ini.get(), "s3", "scheme", &config->s3.scheme);
  IniGet(ini.get(), "s3", "connect_timeout_ms", &config->s3.connect_timeout);
  IniGet(ini.get(), "s3", "request_timeout_ms", &config->s3.request_timeout);

  return arrow::Status::OK();
}

}  // namespace

arrow::Result<std::string> Config::GetFilePath() {
  auto gphome = std::getenv("GPHOME");
  if (!gphome) {
    return arrow::Status::ExecutionError("GPHOME environment variable not set");
  }
  return std::string(gphome) + "/" + kFilePath;
}

arrow::Status Config::FromFile(const std::string& file_path) {
  if (auto ini = ini_load(file_path.c_str())) {
    return ReadValues(IniPointer(ini, ini_free), this);
  } else {
    return arrow::Status::ExecutionError("Cannot load configuration file ",
                                         file_path);
  }
}

arrow::Status Config::FromString(const std::string& content) {
  if (auto ini = ini_load_string(content.c_str())) {
    return ReadValues(IniPointer(ini, ini_free), this);
  } else {
    return arrow::Status::ExecutionError(
        "Cannot load configuration from provided content");
  }
}

}  // namespace iceberg
