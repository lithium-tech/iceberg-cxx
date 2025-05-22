from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from decimal import Decimal
from datetime import date, datetime
from random import randint

spark = SparkSession.builder \
    .getOrCreate()

types = [
    (BooleanType(), True), # boolean
    (ByteType(), 127), # tinyint
    (ShortType(), 32767), # smallint
    (IntegerType(), 2147483647), # int
    (LongType(), 9223372036854775807), # bigint
    (FloatType(), 3.4028235E38), # float
    (DoubleType(), 1.7976931348623157E308), # double
    (DecimalType(10, 2), Decimal(12345.67)), # decimal
    (StringType(), "example"), # string
    (BinaryType(), bytearray(b"spark")), # binary
    (DateType(), date(2023, 10, 10)), # date
    (TimestampType(), datetime(2023, 10, 10, 12, 34, 56)), # timestamp
    (TimestampNTZType(), datetime(2023, 10, 10, 12, 34, 56)), # timestamp_ntz
    (ArrayType(StringType()), ["item1", "item2"]), # array
]

"""(MapType(StringType(), IntegerType()), {"key": 123}), # map
    (StructType([
        StructField("nested_int", IntegerType()),
        StructField("nested_str", StringType())
    ]), (42, "nested")) # struct"""


schema = StructType([
    StructField(f"col{i}", types[i][0]) for i in range(len(types))
])
data = [tuple(t[1] for t in types)]
df = spark.createDataFrame(data, schema)
df.writeTo("demo.performance.schemas") \
    .using("iceberg") \
    .create()


for i in range(1, 2 * len(types) + 1):
    for j in range(len(types)):
        df = df.drop(f"col{i}")
        df.writeTo("demo.performance.schemas") \
            .replace()
        df = df.withColumn(f"col{j}", lit(None).cast(types[(i + j) % len(types)][0]))
        df.writeTo("demo.performance.schemas") \
            .replace()
    for _ in range(3):
        x = randint(0, len(types))
        if (types[(x + i) % len(types)][0] == DecimalType(10, 2)):
            df = df.withColumn(f"col{x}", lit(123.45).cast(DecimalType(10, 2)))
        else:
            df = df.withColumn(f"col{x}", lit(types[(x + i) % len(types)][1]).cast(types[(x + i) % len(types)][0]))
        df.writeTo("demo.performance.schemas") \
            .replace()

spark.sql("DESCRIBE demo.performance.schemas").show(truncate=False)
