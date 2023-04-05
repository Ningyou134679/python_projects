from pyspark.sql.types import *
from pyspark.sql import *

df = [{'id': 1, 'data': {'x': 'mplah', 'y': [10,20,30]}},
      {'id': 2, 'data': {'x': 'mplah2', 'y': [100,200,300]}},
]
schema = StructType([
    StructField('id', LongType()),
    StructField('data', StructType([
        StructField('x', StringType()),
        StructField('y', ArrayType(LongType())),
    ]) )
])

df = spark.createDataFrame(df, schema=schema)
df.printSchema()