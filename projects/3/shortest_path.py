from pyspark.sql.types import *
import pyspark.sql.functions as f
import os
import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

df_schema = StructType(fields = [StructField("user_base", IntegerType()),
    StructField("follower_base", IntegerType())])

df = spark.read.csv(f'{sys.argv[1]}', sep= '\t', schema = df_schema)

def shortest_path(v_from, v_to, df, max_path_length=10):
    k = 1
    df_next_step = df.filter(df.follower_base == v_from)
    df_next_step = df_next_step.select(f.col('user_base').alias('user1'), f.col('follower_base').alias('follower'))
    while k <= max_path_length:
        df_next_step = df.alias('to').join(df_next_step.alias('from'), f.col(f'from.user{k}') == f.col('to.follower_base'), 'inner')
        df_next_step = df_next_step.drop('follower_base')
        df_next_step = df_next_step.withColumnRenamed('user_base', f'user{k+1}')
        if df_next_step.where(f'user{k+1} == {v_to}').count() > 0:
            return df_next_step.where(f'user{k+1} == {v_to}')
        k += 1
    return 0

a = shortest_path(sys.argv[2], sys.argv[3], df)

a = a.select(f.concat_ws(',', *a.columns[::-1]).alias('path'))
a.select("path").write.mode("overwrite").text(str(sys.argv[4]))

