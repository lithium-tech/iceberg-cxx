from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("""
  CREATE TABLE IF NOT EXISTS performance.snapshots (
    id INT
  ) USING iceberg
""")

spark.sql("INSERT INTO performance.snapshots VALUES (0)")
snapshot_df = spark.sql("SELECT snapshot_id FROM performance.snapshots.history ORDER BY made_current_at DESC LIMIT 1")
first_snapshot_id = snapshot_df.collect()[0]["snapshot_id"]

for i in range(1, 1000):
    if i % 2 == 0:
      snapshot_df = spark.sql("SELECT snapshot_id FROM performance.snapshots.history ORDER BY made_current_at DESC LIMIT 1")
      last_snapshot_id = snapshot_df.collect()[0]["snapshot_id"]
      spark.sql(f"ALTER TABLE performance.snapshots CREATE TAG v{i} AS OF VERSION {last_snapshot_id}")
      spark.sql(f"INSERT INTO performance.snapshots VALUES ({i})")
    else:
      spark.sql(f"ALTER TABLE performance.snapshots CREATE BRANCH v{i} AS OF VERSION {first_snapshot_id}")
      spark.sql(f"INSERT INTO performance.snapshots VALUES ({i})")

spark.stop()
