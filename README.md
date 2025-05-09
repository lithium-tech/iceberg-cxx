Apache Iceberg C++ library

# Apache Iceberg™ C++

Yet another C++ implementation of [Apache Iceberg™](https://iceberg.apache.org/).

We started it before [iceberg-cpp](https://github.com/apache/iceberg-cpp) appears. The library was a part of
closed source project for reading Iceberg data via some opensource DBMS we use.
Now we are happy to share results of our work with community to make the best Apache Iceberg™ C++ library together.

## Requirements

- C++20 compliant compiler
- CMake 3.20 or higher
- OpenSSL
- abseil

### Build, Run Test

NOTE: For now you have to [download Apache Arrow dependencies](https://arrow.apache.org/docs/15.0/developers/cpp/building.html#offline-builds) in $ARROW_DEPS directory first.

```bash
mkdir _build
ln -s $ARROW_DEPS _build/arrow-thirdparty
cd _build
cmake -GNinja -DUSE_SMHASHER=ON ../
ninja
cd tests/
../iceberg/iceberg-cpp-test
../iceberg/common/fs/iceberg_common_fs_test
./iceberg_local_test
```
