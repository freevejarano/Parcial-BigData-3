from kafka import KafkaProducer
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions as f
import time

spark = SparkSession.builder.master("local[*]").getOrCreate()
productor = KafkaProducer(bootstrap_servers=['localhost:9092'])

sc=spark.sparkContext
sqlContext = SQLContext(sc)

df= sqlContext.read.csv("Parcial-BigData-3/Segundo/SPY_TICK_TRADE.csv", header="true">
sample = df.rdd.map(lambda x: (x.PRICE))

for row in sample.collect():
    time.sleep(2)
    st=str(row).encode()
    productor.send('quickstart-events',st)

productor.flush()
