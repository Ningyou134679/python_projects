
# coding: utf-8

# In[2]:


from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import *


# In[3]:


from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)


# In[4]:


data = [
    {'id': 1,
     'domain': 'indeed',
     'user': '影134679',
     'pass': 'ランダム033',
     'labels': ['work', 'career', 'recruitment']
     }
]
schema = StructType([
    StructField('id', LongType()),
    StructField('domain', StringType()),
    StructField('user', StringType()),
    StructField('pass', StringType()),
    StructField('labels', ArrayType(StringType())),
])


# In[5]:


df = spark.createDataFrame(data, schema=schema)
df.printSchema()


# In[6]:


df.show(truncate=False)


# In[8]:


output = df.withColumn('labels', col('labels').cast('string'))
#output.write.csv(path='/Downloads/pyspark_test.csv', header="true", mode="overwrite")
output.toPandas().to_csv("/Downloads/test.csv")

