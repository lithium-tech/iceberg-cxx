export SPARK_HOME=/opt/spark

docker cp scripts spark-iceberg:$SPARK_HOME > /dev/null

mkdir -p warehouse/performance

was_generated=0

for filename in scripts/*.py
do
    file=${filename%.py}
    file=${file#scripts/}
    if [ -d "warehouse/performance/$file" ]; then
        echo "skipped $file"
        continue
    fi
    echo "processing $file..."
    #docker exec -it spark-iceberg $SPARK_HOME/bin/spark-submit $SPARK_HOME/$filename # Helps with DEBUG
    res=$(docker exec -it spark-iceberg $SPARK_HOME/bin/spark-submit $SPARK_HOME/$filename)
    if [ $? -eq 0 ];
    then
        was_generated=1
        echo "$file succeed"
    else
        echo $res
        echo "$file failed"
        exit 1
    fi
done

if [ $was_generated -eq 0 ]; then
    exit 0
fi

docker exec -it minio mc alias set myminio http://127.0.0.1:9000 admin password
docker exec -it minio mc cp --recursive myminio/warehouse .

for filename in scripts/*.py
do
    file=${filename%.py}
    file=${file#scripts/}
    if [ -d "warehouse/performance/$file" ]; then
        continue
    fi
    mkdir -p warehouse/performance/$file
    docker cp minio:/warehouse/performance/$file/metadata warehouse/performance/$file
    echo "got $file"


    jsons=(warehouse/performance/$file/metadata/*.json)
    if [ ${#jsons[@]} -eq 0 ]; then
        echo "No metadata file found"
        continue
    fi

    max_json=$(printf '%s\n' "${jsons[@]}" | sort -V | tail -n 1)
    echo $max_json

    for json in "${jsons[@]}"; do
        if [[ "$json" != "$max_json" ]]; then
            rm -- "$json"
        fi
    done

    max_manifest_list=$(grep -oE 'snap-.*\.avro' "$max_json" | tail -1)
    echo "$max_manifest_list"

    for manifest_list in warehouse/performance/$file/metadata/snap-*.avro; do
        if [[ "$manifest_list" != "warehouse/performance/$file/metadata/$max_manifest_list" ]]; then
            rm -- $manifest_list
        fi
    done
done
