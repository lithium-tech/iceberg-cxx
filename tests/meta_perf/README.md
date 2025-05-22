meta_perf_test is a tool for performance testing GetScanMetadata function. 

# Performance Tests

Now iceberg-cxx contains 4 different performance tests to check:

1. Manifest Entries

This is a test case where there is only 1 manifest file, which contains 10,000 manifest entries. Generated using pyspark

2. Manifest Files

This is a test case where there are 10,000 manifest files, each of them contains only 1 manifest entry. Generated using iceberg-cxx

3. Schemas

This is a test case where all supported column types were generated, after which the columns made 2 full circles cyclically shifting, generating a new schema in the metadata. Data was also recorded between cyclic shifts to random columns. Generated using pyspark

4. Snapshots

This is a test case where 1000 small changes took place, each of which generated a new snapshot. Also, 500 tags and 500 branches are created in the test, which somehow point to snapshots.  Generated using payspark

# How to run

Run ./meta_perf_test with the corresponding flags, for example:

./meta_perf_test --manifest_files=8 --full=false

# Usability

You can use this tool for creating FlameGraphs, or for simply measuring average run time
