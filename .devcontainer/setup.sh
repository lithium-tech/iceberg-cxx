#!/bin/bash
set -e

GREEN='\033[0;32m'
NC='\033[0m'

run_all_tests() {
    echo -e "${GREEN}Running all project tests...${NC}"
    (cd _build/tests && ./iceberg_local_test)
    (cd _build && ./iceberg/iceberg-cpp-test)
    (cd _build && ./iceberg/common/fs/iceberg_common_fs_test)
    (cd _build && ./iceberg/equality_delete/iceberg_equality_delete_test)
    (cd _build && ./iceberg/positional_delete/iceberg_positional_delete_test)
    (cd _build && ./iceberg/streams/iceberg_streams_ut)
    (cd _build && ./iceberg/filter/stats_filter/stats_filter_ut)
    (cd _build && ./iceberg/filter/row_filter/row_filter_ut)
    (cd _build && ./tests/meta_perf/meta_perf_test)
    echo -e "${GREEN}All tests passed successfully!${NC}"
}

if [ -x "_build/tests/iceberg_local_test" ]; then
    echo -e "${GREEN}Project is already built. Skipping setup and running tests directly...${NC}"
    run_all_tests
    exit 0
fi

echo -e "${GREEN}Starting setup of Apache Arrow dependencies...${NC}"

mkdir -p _deps
cd _deps

if [ ! -d "arrow" ]; then
    echo -e "${GREEN}Cloning Apache Arrow (maint-15.0.2)...${NC}"
    git clone --single-branch -b maint-15.0.2 https://github.com/apache/arrow.git
fi

cd arrow
echo -e "${GREEN}Checking and applying patches...${NC}"
if ! grep -q "c-ares/releases/download" cpp/thirdparty/versions.txt; then
    echo -e "${GREEN}Applying patch: fix_c-ares_url.patch${NC}"
    git apply ../../vendor/arrow/fix_c-ares_url.patch
fi

if ! grep -q "Decompress(int64_t input_len" cpp/src/arrow/util/compression_snappy.cc; then
    echo -e "${GREEN}Applying patch: arrow-fix-snappy-empty-column.patch${NC}"
    git apply ../../vendor/arrow/arrow-fix-snappy-empty-column.patch
fi

if ! grep -q "ARROW_USE_XSIMD must be TRUE" cpp/cmake_modules/ThirdpartyToolchain.cmake; then
    echo -e "${GREEN}Applying patch: arrow_ensure_xsimd.patch${NC}"
    git apply ../../vendor/arrow/arrow_ensure_xsimd.patch
fi
cd ..

# Always run download_dependencies.sh to verify/download missing files in arrow-thirdparty
mkdir -p arrow-thirdparty
echo -e "${GREEN}Downloading Arrow third-party dependencies...${NC}"
chmod +x ./arrow/cpp/thirdparty/download_dependencies.sh
./arrow/cpp/thirdparty/download_dependencies.sh ./arrow-thirdparty

cd ..

mkdir -p _build

sudo chown -R $(id -u):$(id -g) _build

cd _build

if [ ! -L "arrow-thirdparty" ]; then
    echo -e "${GREEN}Creating symlink for arrow-thirdparty...${NC}"
    ln -sf ../_deps/arrow-thirdparty arrow-thirdparty
fi

echo -e "${GREEN}Setup complete! Build the project...${NC}"
cmake -GNinja ..
ninja

echo -e "${GREEN}Build complete! Running tests...${NC}"
cd ..
run_all_tests
