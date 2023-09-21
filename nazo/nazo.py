
# coding: utf-8

# In[1]:


from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import *


# In[2]:


from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)


# In[3]:


data = [
    {
     'title': 'indeed',
     'user': '影1346xx@gめーる',
     'pass': 'ランダム033',
     'labels': ['website', 'work', 'career', 'recruitment']
     },
    {
     'title': 'マギレコ',
     'user': 'BuRzYDhb',
     'pass': '(N)ななし427',
     'msg': 'us acc',
     'labels': ['game', 'ios', 'マギレコ']
     },
    {
     'title': 'マギレコ',
     'user': 'hHwSyzmT',
     'pass': '(R)ランダム96tanjobi',
     'msg': 'cn acc',
     'labels': ['game', 'ios', 'マギレコ']
     },
    {
     'title': 'asobimo',
     'user': 'ダンテ1346xx@gめーる',
     'pass': '(R)ランダム96tanjobi',
     'labels': ['game', 'ios', 'asobimo']
     },
    {
     'title': 'ラグオリ',
     'user': 'ダンテ1346xx@gめーる',
     'msg': 'ggl login にんぎょう acc',
     'labels': ['game', 'ios', 'ラグオリ']
     },
    {
     'title': 'ラグオリ',
     'user': 'かげ1346xx@gめーる',
     'msg': 'ggl login ヤドカリ acc',
     'labels': ['game', 'ios', 'ラグオリ']
     },
    {
     'title': 'e克斯プレス',
     'user': 'しず112019@outlook',
     'pass': '@らんだむ033',
     'msg': 'EUF3RHKRATSWVFY58ATLFYB',
     'labels': ['vpn']
     },
    {
     'title': '12ビピン',
     'user': 'しず112019@outlook',
     'pass': '@らんだむ96tanjo',
     'msg': 'lamujewupu:tasuluwuge, blockthis.xyz, sk.blockthis.xyz',
     'labels': ['vpn']
     },
    {
     'title': 'ucss',
     'user': 'しず112020@outlook',
     'pass': '@らんだむ_96tanjo',
     'labels': ['vpn']
     },
    {
     'title': 'えっちえすぎんこう',
     'pass': '(96tan)MMddyyyy',
     'msg': 'はーど token',
     'labels': ['work', 'career', 'えっちえすぎんこう']
     },
    {
     'title': 'えっちえすぎんこう',
     'pass': '(96tan)yyyyMM',
     'msg': 'そふと token',
     'labels': ['work', 'career', 'えっちえすぎんこう']
     },
    {
     'title': 'えっちえすぎんこう',
     'user': '450なな7337',
     'pass': '@しごとMMyyyy',
     'msg': 'すたっふcred',
     'labels': ['work', 'career', 'えっちえすぎんこう']
     },
    {
     'title': 'てくSystems',
     'user': 'しず2019@outlook',
     'pass': 'しごと2023',
     'msg': 'timesheet https://allegisgroup-apac1.force.com/community/s/',
     'labels': ['work', 'career', 'あれぎす', 'timesheet']
     },
    {
     'title': 'ringo',
     'user': 'かげ134679@gめーる',
     'pass': '@(N)にんぎょう_96tan',
     'msg': '',
     'labels': ['ios', 'ringo']
     },
    {
     'title': 'ringo',
     'user': 'とり062019@outlook',
     'pass': '@ななし_stanley',
     'msg': '',
     'labels': ['ios', 'ringo']
     },
    {
     'title': 'google',
     'user': 'かげ134679@gmail',
     'pass': '@(N)にんぎょう_96tan',
     'msg': '',
     'labels': ['google']
     },
    {
     'title': 'google',
     'user': 'だんて@gmail',
     'pass': 'せいば9182',
     'msg': '',
     'labels': ['google']
     },
    {
     'title': 'google',
     'user': 'しず134679@gmail',
     'pass': 'せいば9182',
     'msg': '',
     'labels': ['google']
     },
    {
     'title': 'google',
     'user': 'にんぎょう0330@gmail',
     'pass': 'Olive918273xxx',
     'msg': '',
     'labels': ['google']
     },
    {
     'title': 'google',
     'user': 'TestValentine2017@gmail',
     'pass': 'らんどむ021417',
     'msg': '',
     'labels': ['google']
     },
    {
     'title': 'outlook',
     'user': 'しず112019@outlook',
     'pass': '@ななしstanley',
     'msg': 'RHQSF-XSEE4-NMYNW-BQUAV-8XHR2',
     'labels': ['email', 'outlook']
     },
    {
     'title': 'outlook',
     'user': 'とり062019@outlook',
     'pass': '@ななしstanley',
     'msg': 'せきゅ アレックス カラフィナ 202011',
     'labels': ['email', 'outlook']
     },
    {
     'title': 'outlook',
     'user': 'しず112020@outlook',
     'pass': '@ななし_年',
     'labels': ['email', 'outlook']
     },
    {
     'title': 'ちゅうごく半苦',
     'user': 'ろく217857000083921682',
     'msg': 'しはらい 96tanjo99, app 96(N)ねこ96tan',
     'labels': ['ばんく']
     },
    {
     'title': 'ちぇーs',
     'user': 'みなづき1346',
     'pass': 'にんぎょう113',
     'msg': 'しはらい 96tanjo99, app 96(N)ねこ96tan',
     'labels': ['ばんく']
     },
    {
     'title': 'うぃチャット',
     'user': 'いちさんきゅう28857563,しず112020',
     'pass': '@しごと2020',
     'labels': ['app']
     },
    {
     'title': 'jingHigashi',
     'user': 'こたつ11',
     'pass': '@随机stanley',
     'labels': ['app']
     },
    {
     'title': 'google',
     'user': 'しもんしず427@gmail',
     'pass': 'らんどむ427',
     'msg': '',
     'labels': ['google']
     },
    {
     'title': 'mega',
     'user': 'だんて1346xx@gmail',
     'pass': 'らんどむ427',
     'msg': '',
     'labels': ['mega','cloud']
     },
    {
     'title': 'mega',
     'user': 'しず112020@out',
     'pass': '@らんどむ96tan',
     'msg': '',
     'labels': ['mega','cloud']
     },
    {
     'title': 'linkedin',
     'user': 'しず112019@out',
     'pass': 'らんどむ96tan',
     'msg': '',
     'labels': ['career','linkedin','work']
     }

]
schema = StructType([
    StructField('title', StringType()),
    StructField('user', StringType()),
    StructField('pass', StringType()),
    StructField('msg', StringType()),
    StructField('labels', ArrayType(StringType())),
])


# In[8]:


df = spark.createDataFrame(data, schema=schema).withColumn(
    "index",
    row_number().over(Window.orderBy(monotonically_increasing_id()))
).select("index","title","user","pass","msg","labels")
df.printSchema()


# In[9]:


df.show(5,truncate=False)


# In[10]:


df.write.parquet(path='pyspark_test.parquet', mode="overwrite")
output = df.withColumn('labels', col('labels').cast('string'))
output.write.csv(path='pyspark_test.csv', header="true", mode="overwrite")
#output.toPandas().to_csv("test.csv")


# In[11]:


output_df = spark.read.parquet('pyspark_test.parquet', header = True)
print(output_df.count())
output_df.printSchema()
output_df.show(5,False)

