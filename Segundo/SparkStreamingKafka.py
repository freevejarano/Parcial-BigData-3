from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split,avg
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, conv, hex

from pyspark.sql import functions as f

spark = SparkSession \
    .builder \
    .appName("ProgramaAcciones") \
    .getOrCreate()


cols = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("subscribe","quickstart-events") \
    .load()


df=cols.withColumn("Price", conv(col("value"), 16, 16).cast("bigint"))
df2=df.withColumn("PRICE1",df["PRICE"]).withColumn("PRICE2",df["PRICE"])

consulta=df2.agg({'PRICE': 'avg','PRICE1': 'max','PRICE2': 'min'})
consulta=consulta.withColumnRenamed('min(PRICE2)', 'Minimo')
consulta=consulta.withColumnRenamed('max(PRICE1)', 'Maximo')
consulta=consulta.withColumnRenamed('avg(PRICE)', 'Promedio')


writer = query \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start().awaitTermination()
