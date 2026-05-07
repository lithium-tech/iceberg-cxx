#include "iceberg/puffin.h"
#include <gtest/gtest.h>
#include <arrow/io/memory.h>
#include <cstring>

namespace iceberg {

TEST(PuffinFooter, ReadFooterSuccess) {
    std::string payload = "{\"blobs\":[],\"properties\":{}}";
    int32_t payload_size = static_cast<int32_t>(payload.size());
    uint32_t flags = 0;
    
    std::string file_data;
    file_data += kPuffinMagicBytes;
    file_data += payload;
    
    char size_bytes[4];
    std::memcpy(size_bytes, &payload_size, 4);
    file_data.append(size_bytes, 4);
    
    char flags_bytes[4];
    std::memcpy(flags_bytes, &flags, 4);
    file_data.append(flags_bytes, 4);
    
    file_data += kPuffinMagicBytes;
    
    auto reader = std::make_shared<arrow::io::BufferReader>(file_data);
    auto maybe_footer = PuffinFile::ReadFooter(reader);
    
    ASSERT_TRUE(maybe_footer.ok()) << maybe_footer.status().ToString();
    EXPECT_EQ(maybe_footer->GetPayload(), payload);
}

TEST(PuffinFooter, ReadFooterTooSmall) {
    std::string small_data = "short";
    auto reader = std::make_shared<arrow::io::BufferReader>(small_data);
    auto maybe_footer = PuffinFile::ReadFooter(reader);
    EXPECT_FALSE(maybe_footer.ok());
}

TEST(PuffinFooter, ReadFooterInvalidMagic) {
    std::string data(32, 'a');
    auto reader = std::make_shared<arrow::io::BufferReader>(data);
    auto maybe_footer = PuffinFile::ReadFooter(reader);
    EXPECT_FALSE(maybe_footer.ok());
    EXPECT_TRUE(maybe_footer.status().message().find("magic bytes") != std::string::npos);
}

} // namespace iceberg
