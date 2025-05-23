#!/bin/bash

# generate data
mkdir -p $TPCH_DIR/sf25/f1
gen/tpch/tpch-gen --output_dir $TPCH_DIR/sf25/f1 --write_parquet --scale_factor 25

export FILEPATH=$TPCH_DIR/sf25/f1/lineitem0.parquet

# preparet directory for results
export CURRENT_RESULTS_DIR=$ICESTATS_RESULT_DIR/tpch/sf25/f1/
mkdir -p $CURRENT_RESULTS_DIR
