# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.types import *
storage_account_name = "vitordata"
storage_account_key = "gQfr7/QeWyH/J6HDq9I6445udphP/NVckTE5CH4o444WQpKvkNv5fl1SvynIsLIF/pTTvmN0LthuCw/pwtJkkA=="
spark.conf.set(
  "fs.azure.account.key.vitordata.blob.core.windows.net",
  "gQfr7/QeWyH/J6HDq9I6445udphP/NVckTE5CH4o444WQpKvkNv5fl1SvynIsLIF/pTTvmN0LthuCw/pwtJkkA==")

df = spark.read.option("multiline","true").json(('wasbs://vitor-rawdata@vitordata.blob.core.windows.net/twitter_2021/tweet2021.json'))
print(df)

# COMMAND ----------

df = df.select('id', 'text', 'created_at', 'retweet_count')
df.show()

# COMMAND ----------

df.write.parquet('wasbs://vitor-refdata@vitordata.blob.core.windows.net/twitter_2021P1')

# COMMAND ----------

df.registerTempTable('tweetsBolsonaro')
spark.sql("SELECT * FROM tweetsBolsonaro").show()

# COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.vitordata.blob.core.windows.net",
  "gQfr7/QeWyH/J6HDq9I6445udphP/NVckTE5CH4o444WQpKvkNv5fl1SvynIsLIF/pTTvmN0LthuCw/pwtJkkA==")

tweetJson = "wasbs://vitor-rawdata@vitordata.blob.core.windows.net/twitter_2021/tweet2021.json"
readedTweetJson = spark.read.option("multiline","True").json(tweetJson)

df = readedTweetJson.select('id', 'text', 'created_at', 'retweet_count')

df.write.csv('wasbs://vitor-rawdata@vitordata.blob.core.windows.net/twitter_2021/twitter2021_EA')

# COMMAND ----------

pip install pandas

# COMMAND ----------

import pandas as pd

csvtweet2021 = "https://vitordata.blob.core.windows.net/vitor-rawdata/twitter_2021/processed_tweets2021_EA.xls?sp=r&st=2021-06-14T15:36:29Z&se=2021-06-14T23:36:29Z&spr=https&sv=2020-02-10&sr=b&sig=Dk%2B%2FBCbiAXkTMGvo9HaS%2FL6EqwztrZJJ7YPxl8s9qAE%3D"

readedcsv = pd.read_csv(csvtweet2021)

emotional = readedcsv['Classification'].value_counts() 

print(emotional)

# COMMAND ----------

pip install pandas
