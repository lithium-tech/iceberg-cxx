Apache Iceberg C++ library

# Apache Iceberg™ C++

Yet another C++ implementation of [Apache Iceberg™](https://iceberg.apache.org/).

We started it before [iceberg-cpp](https://github.com/apache/iceberg-cpp) appears. The library was a part of
closed source project for reading Iceberg data via some opensource DBMS we use.
Now we are happy to share results of our work with community to make the best Apache Iceberg™ C++ library together.

## Supported features

Source https://iceberg.apache.org/status/

### Data types

| Data type  | Iceberg version | Cxx | Java | Go | Python | Rust |
|--------------------------|---|-----|-----|-----|-----|-----|
| boolean                  | 2 | +   | +   | +   | +   | +   |
| int                      | 2 | +   | +   | +   | +   | +   |
| float                    | 2 | +   | +   | +   | +   | +   |
| double                   | 2 | +   | +   | +   | +   | +   |
| decimal                  | 2 | +   | +   | +   | +   | +   |
| date                     | 2 | +   | +   | +   | +   | +   |
| time                     | 2 | +   | +   | +   | +   | +   |
| timestamp                | 2 | +   | +   | +   | +   | +   |
| timestamptz              | 2 | +   | +   | +   | +   | +   |
| timestamp_ns             | 3 | +   | +   | +   | +   | +   |
| timestamptz_ns           | 3 | +   | +   | +   | +   | +   |
| string                   | 2 | +   | +   | +   | +   | +   |
| uuid                     | 2 | +   | +   | +   | +   | +   |
| fixed                    | 2 | +   | +   | +   | +   | +   |
| binary                   | 2 | +   | +   | +   | +   | +   |
| variant                  | 3 | -   | +   | +   | +   | +   |
| list                     | 2 | +   | +   | +   | +   | +   |
| map                      | 2 | -   | +   | +   | +   | +   |
| struct                   | 2 | -   | +   | +   | +   | +   |
| unknown                  | 3 | +   | ?   | ?   | ?   | ?   |

### Data files format

| File format   | Cxx | Java | Go | Python | Rust |
|---------------|-----|-----|-----|-----|-----|
| Parquet       | +   | +   | +   | +   | +   |
| ORC           | -   | +   | -   | -   | -   |
| Puffin        | +   | +   | -   | -   | -   |
| Avro          | -   | +   | -   | -   | -   |

### File IO

| File IO            | Cxx | Java | Go | Python | Rust |
|--------------------|-----|-----|-----|-----|-----|
| Local Filesystem   | +   | +   | +   | +   | +   |
| Hadoop Filesystem  | -   | +   | +   | +   | +   |
| S3 Compatible      | +   | +   | +   | +   | +   |
| GCS Compatible     | ?   | +   | +   | +   | +   |
| ADLS Compatible    | -   | +   | +   | +   | -   |

### Table Maintenance Operations

Not implemented

### Table Update Operations

Not implemented

### Table read operations

| Operation        | Iceberg version | Cxx | Java | Go | Python | Rust |
|------------------------------|-----|-----|-----|-----|-----|-----|
| Plan with data file          | 1,2 | +   | +   | +   | +   | +   |
| Plan with position deletes   |   2 | +   | +   | +-  | +   | -   |
| Plan with equality deletes   |   2 | +   | +   | +-  | +   | -   |
| Plan with puffin statistics  | 1,2 | -   | +   | -   | -   | -   |
| Read data file               | 1,2 | +   | +   | +   | +   | +   |
| Read with position deletes   |   2 | +   | +   | +   | +   | -   |
| Read with equality deletes   |   2 | +   | +   | -   | -   | -   |

### Table write operations

| Operation     | Iceberg version | Cxx | Java | Go | Python | Rust |
|---------------------------|-----|-----|-----|-----|-----|-----|
| Append data               | 1,2 | +   | +   | +   | +   | +   |
| Write position deletes    |   2 | -   | +   | -   | -   | -   |
| Write equality deletes    |   2 | -   | +   | -   | -   | -   |
| Write deletion vectors    |   3 | +   | +   | -   | -   | -   |

### Catalog

| Table Operation           | Rest | Glue | HMS |
|----------------------------|-----|-----|-----|
| listTable                  | -   | -   | -   |
| createTable                | -   | -   | -   |
| dropTable                  | -   | -   | -   |
| loadTable                  | +-  | -   | +-  |
| updateTable                | -   | -   | -   |
| renameTable                | -   | -   | -   |
| tableExists                | +-  | -   | +-  |
| createView                 | -   | -   | -   |
| dropView                   | -   | -   | -   |
| listView                   | -   | -   | -   |
| viewExists                 | -   | -   | -   |
| replaceView                | -   | -   | -   |
| renameView                 | -   | -   | -   |
| listNamespaces             | -   | -   | -   |
| createNamespace            | -   | -   | -   |
| dropNamespace	             | -   | -   | -   |
| namespaceExists            | -   | -   | -   |
| updateNamespaceProperties  | -   | -   | -   |
| loadNamespaceMetadata      | -   | -   | -   |

## Requirements

- C++20 compliant compiler
- CMake 3.20 or higher
- OpenSSL

### Build, Run Test

You have to [download Apache Arrow dependencies](https://arrow.apache.org/docs/15.0/developers/cpp/building.html#offline-builds) first.
```bash
mkdir _deps && cd _deps
git clone --single-branch -b maint-15.0.2 https://github.com/apache/arrow.git
cd arrow && git apply ../../vendor/arrow/fix_c-ares_url.patch && cd ..
./arrow/cpp/thirdparty/download_dependencies.sh ./arrow-thirdparty
```

```bash
mkdir _build
cd _build
ln -s ../_deps/arrow-thirdparty arrow-thirdparty
cmake -GNinja ../
ninja
cd tests/
../iceberg/iceberg-cpp-test
../iceberg/common/fs/iceberg_common_fs_test
./iceberg_local_test
```
