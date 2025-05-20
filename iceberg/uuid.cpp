#include "iceberg/uuid.h"

#include <algorithm>
#include <ifaddrs.h>
#include <net/if.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <unistd.h>

#ifdef __APPLE__
#include <net/if_dl.h>
#endif

#include <array>
#include <chrono>
#include <cstring>
#include <mutex>
#include <stdexcept>

namespace iceberg {

namespace {

static constexpr std::array<size_t, 4> kSeparateIndices{8, 13, 18, 23};
static constexpr const char* kDigits = "0123456789abcdef";
static constexpr int kMinUuidLength = 32;
static constexpr int kMaxUuidLength = 36;

static constexpr int kInt8Mask = 0xF;
static constexpr int kInt16Mask = 0xFF;
static constexpr int kInt32Mask = 0xFFFF;

static constexpr int kMaxTicks = 100;

}  // namespace

Uuid::Uuid() : time_low_(0), time_mid_(0), time_hi_and_version_(0), clock_seq_(0) {
  std::fill(node_, node_ + sizeof(node_), 0);
}

Uuid::Uuid(uint32_t time_low) : time_low_(time_low) {}

Uuid::Uuid(const std::string& uuid) { Parse(uuid); }

void Uuid::Swap(Uuid& uuid) {
  std::swap(time_low_, uuid.time_low_);
  std::swap(time_mid_, uuid.time_mid_);
  std::swap(time_hi_and_version_, uuid.time_hi_and_version_);
  std::swap(clock_seq_, uuid.clock_seq_);
  std::swap_ranges(node_, node_ + sizeof(node_), &uuid.node_[0]);
}

void Uuid::Parse(const std::string& uuid) {
  auto throw_error = [&](const std::string& description) {
    throw std::runtime_error("Incorrect uuid " + uuid + " : " + description);
  };

  if (uuid.size() < kMinUuidLength)
    throw_error("length should be greater or equals than " + std::to_string(kMinUuidLength));

  bool haveHyphens = false;
  if (std::all_of(kSeparateIndices.cbegin(), kSeparateIndices.cend(), [&](int ind) { return uuid[ind] == '-'; })) {
    if (uuid.size() >= kMaxUuidLength)
      haveHyphens = true;
    else
      throw_error("incorrect separators positions");
  }

  Uuid new_uuid;
  std::string::const_iterator it = uuid.begin();

  auto setField = [&]<typename T>(T& field, const std::string& field_description) {
    field = 0;
    for (size_t i = 0; i < 2 * sizeof(T); ++i) {
      int16_t n = nibble(*it++);
      if (n < 0) throw_error("error while parsing field" + field_description);
      field = (field << 4) | n;
    }
    if (haveHyphens) ++it;
  };

  setField(new_uuid.time_low_, "time_low");
  setField(new_uuid.time_mid_, "time_mid");
  setField(new_uuid.time_hi_and_version_, "time_hi_and_version");
  setField(new_uuid.clock_seq_, "clock_seq");

  for (size_t i = 0; i < sizeof(node_); ++i) {
    int16_t n1 = nibble(*it++);
    if (n1 < 0) throw_error("incorrect mac address");
    int16_t n2 = nibble(*it++);
    if (n2 < 0) throw_error("incorrect mac address");

    new_uuid.node_[i] = (n1 << 4) | n2;
  }
  Swap(new_uuid);
}

void Uuid::AppendHex(std::string& str, uint8_t n) {
  int offset = 4 * sizeof(n);
  str += kDigits[(n >> offset) & kInt8Mask];
  str += kDigits[n & kInt8Mask];
}

void Uuid::AppendHex(std::string& str, uint16_t n) {
  int offset = 4 * sizeof(n);
  AppendHex(str, static_cast<uint8_t>(n >> offset));
  AppendHex(str, static_cast<uint8_t>(n & kInt16Mask));
}

void Uuid::AppendHex(std::string& str, uint32_t n) {
  int offset = 4 * sizeof(n);
  AppendHex(str, static_cast<uint16_t>(n >> offset));
  AppendHex(str, static_cast<uint16_t>(n & kInt32Mask));
}

std::string Uuid::ToString() const {
  std::string result;
  result.reserve(kMaxUuidLength);
  AppendHex(result, time_low_);
  result += '-';
  AppendHex(result, time_mid_);
  result += '-';
  AppendHex(result, time_hi_and_version_);
  result += '-';
  AppendHex(result, clock_seq_);
  result += '-';
  for (size_t i = 0; i < sizeof(node_); ++i) {
    AppendHex(result, node_[i]);
  }
  return result;
}

bool Uuid::IsNull() const {
  bool result = time_low_ == 0 && time_mid_ == 0 && time_hi_and_version_ == 0 && clock_seq_ == 0;

  for (size_t i = 0; i < sizeof(node_); ++i) {
    result &= (node_[i] == 0);
  }
  return result;
}

Uuid::Version Uuid::GetVersion() const { return Uuid::Version(time_hi_and_version_ >> 12); }

int16_t Uuid::nibble(char hex) {
  if (hex >= 'a' && hex <= 'f')
    return hex - 'a' + 10;
  else if (hex >= 'A' && hex <= 'F')
    return hex - 'A' + 10;
  else if (hex >= '0' && hex <= '9')
    return hex - '0';
  else
    return -1;
}

Uuid::Uuid(uint32_t time_low, uint32_t time_mid, uint32_t time_hi_and_version, uint16_t clock_seq, uint8_t node[])
    : time_low_(time_low), time_mid_(time_mid), time_hi_and_version_(time_hi_and_version), clock_seq_(clock_seq) {
  std::memcpy(node_, node, sizeof(node_));
}

UuidGenerator::UuidGenerator(Uuid::Version version) : version_(version) {
  if (version == Uuid::Version::kMacless) {
    for (int i = 0; i < 6; ++i) {
      node_[i] = 0;
    }
    return;
  }
  if (!SetMacAddress()) {
    throw std::runtime_error("Error while getting mac address of node");
  }
}

bool UuidGenerator::SetMacAddress() {
#ifdef __APPLE__
  struct ifaddrs *ifap, *ifa;
  struct sockaddr_dl* sdl;

  if (getifaddrs(&ifap) != 0) {
    return false;
  }

  bool found = false;

  for (ifa = ifap; ifa != nullptr; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr->sa_family == AF_LINK && std::strcmp(ifa->ifa_name, "en0") == 0) {
      sdl = (struct sockaddr_dl*)ifa->ifa_addr;
      std::memcpy(node_, LLADDR(sdl), sdl->sdl_alen);
      found = true;
      break;
    }
  }

  freeifaddrs(ifap);
  return found;
#else

  struct ifreq ifr;
  int sock = socket(AF_INET, SOCK_DGRAM, 0);

  if (sock == -1) {
    return false;
  }

  std::strncpy(ifr.ifr_name, "eth0", IFNAMSIZ - 1);
  ifr.ifr_name[IFNAMSIZ - 1] = '\0';

  if (ioctl(sock, SIOCGIFHWADDR, &ifr) == -1) {
    close(sock);
    return false;
  }

  close(sock);

  std::memcpy(node_, ifr.ifr_hwaddr.sa_data, sizeof(node_));
  return true;
#endif
}

uint64_t UuidGenerator::GetTimestamp() {
  auto now = std::chrono::system_clock::now();
  while (true) {
    if (now != timestamp_) {
      timestamp_ = now;
      ticks_ = 0;
      break;
    }
    if (ticks_ < kMaxTicks) {
      ++ticks_;
      break;
    }
    now = std::chrono::system_clock::now();
  }
  now = std::chrono::system_clock::now();
  auto value = std::chrono::duration_cast<std::chrono::microseconds>(now.time_since_epoch());
  auto duration = value.count();
  return duration + ticks_;
}

Uuid UuidGenerator::CreateRandom() {
  uint64_t tv;

  // it is assumed that the method will not be called so often, so you can use a usual mutex
  {
    std::lock_guard lock(mutex_);

    tv = GetTimestamp();
  }
  auto time_low = static_cast<uint32_t>(tv & 0xFFFFFFFF);
  auto time_mid = static_cast<uint64_t>((tv >> 32) & 0xFFFF);
  auto time_hi_and_version = static_cast<uint16_t>((tv >> 48) & 0x0FFF) + (static_cast<int>(version_) << 12);

  auto clock_seq = (static_cast<uint16_t>(rng_() >> 4) & 0x3FFF) | 0x8000;

  return Uuid(time_low, time_mid, time_hi_and_version, clock_seq, node_);
}

}  // namespace iceberg
