# Databricks notebook source
import numpy as np
import pandas as pd

int_list = (pd.DataFrame(np.arange(0,250, dtype=int)))[::-1]
df=spark.createDataFrame(int_list)
df.show()

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('ANIMALS').getOrCreate()

animais = ['jacaré'],['leao'],['girafa'],['lobo'],['rinoceronte'],['bufalo'],['zebra'],['cavalo'],['anta'],['ornitorrinco'],['pato'],['galinha'],['tigre'],['cervo'],['urso'],['crocodilo'],['tubarao'],['elefante'],['baleia'],['golfinho']

columns = []
animals = (spark.createDataFrame(data=animais, schema=columns))
animals.show()
#df.write.csv('animais.csv', header=True)
type(animals)




# COMMAND ----------

spark.read.csv('dbfs:/df.csv', header=True).show()

# COMMAND ----------

pip install names

# COMMAND ----------

pip install names

# COMMAND ----------

import random
import time
import names
import os

storage_account_name = "vitordata"
storage_account_key = "gQfr7/QeWyH/J6HDq9I6445udphP/NVckTE5CH4o444WQpKvkNv5fl1SvynIsLIF/pTTvmN0LthuCw/pwtJkkA=="
spark.conf.set(
  "fs.azure.account.key.vitordata.blob.core.windows.net",
  "gQfr7/QeWyH/J6HDq9I6445udphP/NVckTE5CH4o444WQpKvkNv5fl1SvynIsLIF/pTTvmN0LthuCw/pwtJkkA==")

t0= time.time()
random.seed(42)

qtde_nomes_unicos = 3000
qtde_nomes_aleatorios = 100000


# COMMAND ----------

aux=[]
for i in range(0, qtde_nomes_unicos):
  aux.append(names.get_full_name())
  
print("Gerando {} nomes aleatorios".format(qtde_nomes_aleatorios))
dados = []
for i in range(0, qtde_nomes_aleatorios):
  dados.append(random.choice(aux))

# COMMAND ----------

type(dados)


# COMMAND ----------

print(dados)

# COMMAND ----------

print("gravando em arquivos")

arquivo = open('/dbfs/FileStore/nomes.txt', 'w')
for item in dados:
  arquivo.write(item + '\n')
arquivo.close()

tf = time.time() -t0
print("Criacao FInalizada em {} segundos".format(tf))


# COMMAND ----------

dbfs cp /dbfs/FileStore/nomes.txt

# COMMAND ----------

txt=spark.read.text('dbfs:/nomes.txt')
display(txt)

# COMMAND ----------

ls

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkContext, SQLContext
from pyspark.sql import SparkSession
storage_account_name = "vitordata"
storage_account_key = "gQfr7/QeWyH/J6HDq9I6445udphP/NVckTE5CH4o444WQpKvkNv5fl1SvynIsLIF/pTTvmN0LthuCw/pwtJkkA=="
spark.conf.set(
  "fs.azure.account.key.vitordata.blob.core.windows.net",
  "gQfr7/QeWyH/J6HDq9I6445udphP/NVckTE5CH4o444WQpKvkNv5fl1SvynIsLIF/pTTvmN0LthuCw/pwtJkkA==")


spark = SparkSession \
  .builder \
  .appName("Exercicios")\
  .getOrCreate()

sqlContext = SQLContext(spark.sparkContext)



df_nomes = spark.read.csv('wasbs://vitor-rawdata@vitordata.blob.core.windows.net/nomecsv/nomes.csv', header=True)

df_nomes.show

# COMMAND ----------

df_nomes.show(5)

# COMMAND ----------

df_nomes.printSchema()

# COMMAND ----------

df_nomes.select(df_nomes.nome, df_nomes.sexo).show(5)

# COMMAND ----------

df_select = df_nomes.select(df_nomes.nome, 
                            df_nomes.ano,
                            df_nomes.ano > 1950,
                            df_nomes.ano + 1000
                           )
df_select.show(5)

# COMMAND ----------

df_nomes.registerTempTable("pessoas")
sqlContext.sql("select count(*) from pessoas").show()

# COMMAND ----------

sqlContext.sql("SELECT nome, ano, ano+1000 as futuro FROM pessoas").show(5)

# COMMAND ----------

df_nomes.filter(df_nomes.ano > 1990).select(df_nomes.nome, df_nomes.ano).show(5)

# COMMAND ----------

df_nomes.filter(df_nomes.ano > 1990)\
  .select (df_nomes.nome, df_nomes.ano)\
  .orderBy (df_nomes.ano, df_nomes.nome).show(5)

# COMMAND ----------

df_nomes.filter(df_nomes.ano > 1990)\
  .select (df_nomes.nome, df_nomes.ano)\
  .orderBy (df_nomes.ano.desc(), df_nomes.nome.desc()).show(5)

# COMMAND ----------

df_nomes.select("nome", "ano",(df_nomes.ano>2000).alias('recene')).show(5)

# COMMAND ----------

spark.sql("SELECT nome, ano, ano > 2000 AS recente FROM pessoas").show(5)

# COMMAND ----------

df_nomes.groupBy('ano').count().show(5)

# COMMAND ----------

#EXERCICIO 1
df_sexo=df_nomes.groupBy('sexo').count().show()



feminino=df_nomes.filter(df_nomes['sexo'] == 'F').count()
masculino=df_nomes.filter(df_nomes['sexo'] == 'M').count()
diferenca=feminino-masculino
print("diferença entre sexos {}".format(diferenca))

# COMMAND ----------

df_nomes.select("nome", (df_nomes.total).alias("ranking")).display()
  


  

# COMMAND ----------



df_nomes.select("ano",
               ((df_nomes.ano >= 1980) & 
                (df_nomes.ano <= 1989)).alias("DECADA_80"),
                
                ((df_nomes.ano >= 1991) & 
                (df_nomes.ano <= 1999)).alias("DECADA_90"),
                
                ((df_nomes.ano >= 1991) & 
                (df_nomes.ano <= 2000)).alias("DECADA_00"),
               
               ).show()
  
  
