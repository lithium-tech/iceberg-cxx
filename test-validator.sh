#!/bin/bash

TEST_DATA_DIR="tests/warehouse"
MINIO=${MINIO_EXECUTABLE:-minio}
MC=${MC_EXECUTABLE:-mc}
HOST=127.0.0.1
HMS_PORT=9090
S3_PORT=9000
DB_NAME=miniodb

export AWS_ENDPOINT_URL="http://$HOST:$S3_PORT"
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
export AWS_DEFAULT_REGION=""

mkdir $MINIO_DATA_DIR
$MINIO server $MINIO_DATA_DIR &

sleep 1

echo $AWS_ENDPOINT_URL $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY
$MC alias set 'myminio' $AWS_ENDPOINT_URL $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY
$MC mb warehouse
$MC mirror $TEST_DATA_DIR warehouse
$MC ls warehouse

$MC ls -r warehouse
