from pyspark.sql.session import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder \
    .getOrCreate()


spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.performance.manifest_entries (
        id BIGINT,
        data STRING
    ) USING iceberg
    TBLPROPERTIES(
        'commit.manifest-merge.enabled' = 'false',
        'write.target-file-size-bytes' = '1024'
    )
""")

df = spark.range(0, 10000000).withColumn("data", lit("sample_data"))

df.writeTo("demo.performance.manifest_entries") \
    .append()
