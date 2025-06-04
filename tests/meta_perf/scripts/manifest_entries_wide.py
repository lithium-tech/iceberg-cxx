from pyspark.sql.session import SparkSession
from pyspark.sql.functions import lit

spark = SparkSession.builder \
    .getOrCreate()


spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.performance.manifest_entries_wide (
        id BIGINT,
        id0 BIGINT,
        id1 BIGINT,
        id2 BIGINT,
        id3 BIGINT,
        id4 BIGINT,
        id5 BIGINT,
        id6 BIGINT,
        id7 BIGINT,
        id8 BIGINT,
        id9 BIGINT,
        id10 BIGINT,
        id11 BIGINT,
        id12 BIGINT,
        id13 BIGINT,
        id14 BIGINT,
        id15 BIGINT,
        id16 BIGINT,
        id17 BIGINT,
        id18 BIGINT,
        id19 BIGINT,
        id20 BIGINT,
        id21 BIGINT,
        id22 BIGINT,
        id23 BIGINT,
        id24 BIGINT,
        id25 BIGINT,
        id26 BIGINT,
        id27 BIGINT,
        id28 BIGINT,
        id29 BIGINT,
        id30 BIGINT,
        id31 BIGINT,
        id32 BIGINT,
        id33 BIGINT,
        id34 BIGINT,
        id35 BIGINT,
        id36 BIGINT,
        id37 BIGINT,
        id38 BIGINT,
        id39 BIGINT,
        id40 BIGINT,
        id41 BIGINT,
        id42 BIGINT,
        id43 BIGINT,
        id44 BIGINT,
        id45 BIGINT,
        id46 BIGINT,
        id47 BIGINT,
        id48 BIGINT,
        id49 BIGINT
    ) USING iceberg
    TBLPROPERTIES(
        'commit.manifest-merge.enabled' = 'false',
        'write.target-file-size-bytes' = '1024'
    )
""")

df = spark.range(0, 10000000 / 50) \
    .withColumn("id0", lit(2101)) \
    .withColumn("id1", lit(2101)) \
    .withColumn("id2", lit(2101)) \
    .withColumn("id3", lit(2101)) \
    .withColumn("id4", lit(2101)) \
    .withColumn("id5", lit(2101)) \
    .withColumn("id6", lit(2101)) \
    .withColumn("id7", lit(2101)) \
    .withColumn("id8", lit(2101)) \
    .withColumn("id9", lit(2101)) \
    .withColumn("id10", lit(2101)) \
    .withColumn("id11", lit(2101)) \
    .withColumn("id12", lit(2101)) \
    .withColumn("id13", lit(2101)) \
    .withColumn("id14", lit(2101)) \
    .withColumn("id15", lit(2101)) \
    .withColumn("id16", lit(2101)) \
    .withColumn("id17", lit(2101)) \
    .withColumn("id18", lit(2101)) \
    .withColumn("id19", lit(2101)) \
    .withColumn("id20", lit(2101)) \
    .withColumn("id21", lit(2101)) \
    .withColumn("id22", lit(2101)) \
    .withColumn("id23", lit(2101)) \
    .withColumn("id24", lit(2101)) \
    .withColumn("id25", lit(2101)) \
    .withColumn("id26", lit(2101)) \
    .withColumn("id27", lit(2101)) \
    .withColumn("id28", lit(2101)) \
    .withColumn("id29", lit(2101)) \
    .withColumn("id30", lit(2101)) \
    .withColumn("id31", lit(2101)) \
    .withColumn("id32", lit(2101)) \
    .withColumn("id33", lit(2101)) \
    .withColumn("id34", lit(2101)) \
    .withColumn("id35", lit(2101)) \
    .withColumn("id36", lit(2101)) \
    .withColumn("id37", lit(2101)) \
    .withColumn("id38", lit(2101)) \
    .withColumn("id39", lit(2101)) \
    .withColumn("id40", lit(2101)) \
    .withColumn("id41", lit(2101)) \
    .withColumn("id42", lit(2101)) \
    .withColumn("id43", lit(2101)) \
    .withColumn("id44", lit(2101)) \
    .withColumn("id45", lit(2101)) \
    .withColumn("id46", lit(2101)) \
    .withColumn("id47", lit(2101)) \
    .withColumn("id48", lit(2101)) \
    .withColumn("id49", lit(2101))


df.writeTo("demo.performance.manifest_entries_wide") \
    .append()
