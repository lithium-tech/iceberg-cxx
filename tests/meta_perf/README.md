./meta_perf_test is a tool for performance testing GetScanMetadata function. 

# Performance Tests

Now iceberg-cxx contains 4 different performance tests to check:

### Manifest Entries

This is a test case where there is only 1 manifest file, which contains 10,000 manifest entries. Generated using pyspark

### Manifest Files

This is a test case where there are 10,000 manifest files, each of them contains only 1 manifest entry. Generated using iceberg-cxx

### Schemas

This is a test case where all supported column types were generated, after which the columns made 2 full circles cyclically shifting, generating a new schema in the metadata. Data was also recorded between cyclic shifts to random columns. Generated using pyspark

### Snapshots

This is a test case where 1000 small changes took place, each of which generated a new snapshot. Also, 500 tags and 500 branches are created in the test, which somehow point to snapshots.  Generated using pyspark

# How to run

Run ./meta_perf_test with the corresponding flags, for example:

```./meta_perf_test --manifest_files=8 --full=false```

# Usability

You can use this tool for profiling CPU usage during GetScanMetadata function. Here is a short guide how to do this on Linux:

1. Download linux-common-tools, which contains perf util

2. Build the project, go to tests/meta_perf and run:

```
sudo perf record -F 200 -g --call-graph fp -o path_to_perf_data/perf.data ./meta_perf_test --manifest_entries=200 --full=false
```

where _path_to_perf_data_ is a path, where you want to save perf.data file.

3. Get stackcollapse-perf.pl and flamegraph.pl scripts from https://github.com/brendangregg/FlameGraph. 

4. Go to _path_to_perf_data_ and run:

```
sudo perf script | path_to_scripts/stackcollapse-perf.pl | path_to_scripts/flamegraph.pl > flamegraph.svg
```

where _path_to_scripts_ is a path to stackcollapse-perf.pl and flamegraph.pl scripts. 

5. You will see the result in flamegraph.svg.

One more thing this tool can be helpful for is simply measuring average run time. You can compare different GetScanMetadata realizations' run times with ministat Linux util. 
