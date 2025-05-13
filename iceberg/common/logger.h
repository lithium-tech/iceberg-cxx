#pragma once

#include <string>

namespace iceberg {

class ILogger {
 public:
  using Message = std::string;
  using MessageType = std::string;

  virtual void Log(const Message& message, const MessageType& message_type) = 0;

  virtual ~ILogger() = default;
};

}  // namespace iceberg
