#pragma once

#include <stdint.h>

#include <mutex>
#include <random>
#include <string>
#include <utility>

namespace iceberg {

/*
 * Uuid and UuidGenerator are modified by poco library
 *
 * https://github.com/ClickHouse/ClickHouse/blob/master/base/poco/Foundation/src/UUID.cpp
 * https://github.com/ClickHouse/ClickHouse/blob/master/base/poco/Foundation/src/UUIDGenerator.cpp
 */

class Uuid {
 public:
  enum class Version {
    kUuidTimeBased = 1,
    kUuidDceUid = 2,
    kUuidNameBased = 3,
    kUuidRandom = 4,
    kUuidNameBasedSha1 = 5
  };

  // Create null uuid
  Uuid();

  explicit Uuid(const std::string& uuid);

  void Swap(Uuid& uuid);
  void Parse(const std::string& uuid);

  std::string ToString() const;

  Version GetVersion() const;

  std::strong_ordering operator<=>(const Uuid& uuid) const = default;

  bool operator!=(const Uuid& uuid) const = default;

  bool IsNull() const;

  ~Uuid() = default;

 private:
  int16_t nibble(char c);

  static void AppendHex(std::string& str, uint8_t n);
  static void AppendHex(std::string& str, uint16_t n);
  static void AppendHex(std::string& str, uint32_t n);

  explicit Uuid(uint32_t time_low, uint32_t time_mid, uint32_t time_hi_and_version, uint16_t clock_seq, uint8_t node[]);

  uint32_t time_low_;
  uint16_t time_mid_;
  uint16_t time_hi_and_version_;
  uint16_t clock_seq_;
  uint8_t node_[6];

  friend class UuidGenerator;
};

static_assert(8 * sizeof(Uuid) == 128);

class UuidGenerator {
 public:
  explicit UuidGenerator(Uuid::Version version = Uuid::Version::kUuidTimeBased);

  ~UuidGenerator() = default;

  Uuid CreateRandom();

 private:
  uint64_t GetTimestamp();
  bool SetMacAddress();

  Uuid::Version version_;

  std::mutex mutex_;
  std::mt19937 rng_;
  std::chrono::system_clock::time_point timestamp_;
  int ticks_;
  uint8_t node_[6];
};

}  // namespace iceberg
