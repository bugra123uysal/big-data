

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count
import pandas as pd
from pyspark.sql.functions import split, unix_timestamp,when, col, sum ,year, month, dayofweek, hour,date_format ,  mean
from datetime import time
import plotly.express as px

from google.colab import files
!pip install keplergl
from keplergl import KeplerGl

spark=SparkSession.builder.appName("first big data").getOrCreate()

df_sp1501=spark.read.csv("/content/sample_data/yellow_tripdata_2015-01.csv", header=True , inferSchema=True)
df_sp1601=spark.read.csv("/content/sample_data/yellow_tripdata_2016-01.csv", header=True , inferSchema=True)
df_sp1602=spark.read.csv("/content/sample_data/yellow_tripdata_2016-02.csv", header=True , inferSchema=True)
df_sp1603=spark.read.csv("/content/sample_data/yellow_tripdata_2016-03.csv", header=True , inferSchema=True)

df_sp=df_sp1501.union(df_sp1601).union(df_sp1602).union(df_sp1603)
df_sp.show(5)

df_sp.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_sp.columns]).show()

df_sp=df_sp.select([count(when(col(c).isNull(),c)).alias(c)for c in df_sp.columns]).show()

df_sp.summary().show()

df_sp.show(5)

df_sp.printSchema()

df_sp.orderBy(col("tip_amount").asc()).select("trip_distance", "fare_amount", "total_amount").show(1)

df_sp.orderBy(col("trip_distance").desc()).select("trip_distance", "fare_amount", "total_amount").show(1)

df_sp.orderBy(col("total_amount").desc()).select("trip_distance", "total_amount", "tip_amount").show(5)

df_sp.orderBy(col("total_amount").asc()).select("trip_distance", "total_amount", "tip_amount").show(5)

df_sp.groupBy("passenger_count").count().orderBy("count", ascending=False).show()

df_sp.groupBy("passenger_count").count().orderBy("count", ascending=True ).show()

df_sp.orderBy("passenger_count").count().orderBy("count", ascending=False).show()

df_sp.orderBy("payment_type").count().orderBy("count", ascending=False).show(5)

df_sp=df_sp.orderBy(df_sp['passenger_count'], ascending=False)

df_sp=df_sp.select("payment_type")

gecıs=df_sp.orderBy(df_sp['tolls_amount'], ascending=False)

df_sp.select(mean("tip_amount")).show(5)

df_sp.select(mean("total_amount")).show(5)

df_sp = df_sp[df_sp["passenger_count"] > 1]
df_sp.show(5)

df_sp.count()

df_sp =df_sp.withColumn("pickup_datetime", split(df_sp["tpep_pickup_datetime"], " ")[1])\
             .withColumn("dropoff_datetime", split(df_sp["tpep_dropoff_datetime"], " ")[1])

df_sp = df_sp.withColumn("time_different_minute",
                         (unix_timestamp("tpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss") -
                          unix_timestamp("tpep_pickup_datetime", "yyyy-MM-dd HH:mm:ss")) / 60)


df_sp.select("tpep_pickup_datetime", "tpep_dropoff_datetime", "time_different_minute")

df_sp=df_sp.withColumn("fare_per_minute", col("fare_amount") / col("time_different_minute"))
df_sp=df_sp.withColumn("fare_per_km", col("fare_amount") / col("trip_distance"))

df_sp=df_sp.withColumn("year", year(df_sp["tpep_pickup_datetime"]))
df_sp=df_sp.withColumn("month", month(df_sp["tpep_pickup_datetime"]))
df_sp=df_sp.withColumn("hour", hour(df_sp["tpep_pickup_datetime"]))
df_sp=df_sp.withColumn("dayofweek", dayofweek(df_sp["tpep_pickup_datetime"]))


ccoo=df_sp.groupBy("hour").count().toPandas()
df_sp=px.bar(ccoo , x="hour", y="count", title="hourse ,(tpep_pickup_datetime)")

df_sp=df_sp.withColumn("payment",
                       when(df_sp.payment_type == 1 , "cash")
                    .when(df_sp.payment_type == 2 , "credid card")
                    .otherwise("ı dont know")
                    )


aa=df_sp.groupBy("payment").count().toPandas()
df_sp=px.bar(aa, x="payment", y="count")

df_sp=df_sp.withColumn("year_d", year(df_sp["tpep_dropoff_datetime"]))
df_sp=df_sp.withColumn("month_d", month(df_sp["tpep_dropoff_datetime"]))
df_sp=df_sp.withColumn("hour_d", hour(df_sp["tpep_dropoff_datetime"]))
df_sp=df_sp.withColumn("dayofweek_d", dayofweek(df_sp["tpep_dropoff_datetime"]))


count_d=df_sp.groupBy("hour_d").count().toPandas()
df_sp=px.bar(count_d, x="hour_d", y="count", title="hours (tpep_dropoff_datetime)" )


df_sp=df_sp.filter((col("hour_d")>= 17) & (col("hour_d")<= 20)).show(10)

df_sp=df_sp.withColumn("month_s",
                         when(col("month") == 1 , "ocak")
                       .when(col("month")==  2 , "şubat")
                       .when(col("month") == 3 ,"mart")
                       .when(col("month") == 4 ,"nisan")
                       .when(col("month") == 5 , "mayıs")
                       .when(col("month") == 6 , "haziran")
                       .when(col("month") == 7 ,"temmuz")
                       .when(col("month") == 8 , "ağustos")
                       .when(col("month") == 9 , "eylül")
                       .when(col("month") == 10, "ekim")
                       .when(col("month") == 11, "kasım")
                       .when(col("month") == 12, "aralık")
                       .otherwise("try agen")


                       )



aa=df_sp.groupBy("month_s").count().toPandas()
df_sp=px.bar(aa, x="month_s" , y="count")




df_sp = df_sp.filter(df_sp["month_s"].isin(['ocak']))

df_sp=df_sp.withColumn("dayofweek_d", dayofweek(df_sp["tpep_dropoff_datetime"]))
df_sp=df_sp.withColumn("day_name",

                       when(df_sp.dayofweek == 1, "pazartesı")
                    .when(df_sp.dayofweek == 2 , "salı")
                    .when(df_sp.dayofweek ==  3, "çarşamba")
                    .when(df_sp.dayofweek ==  4, "perşembe")
                    .when(df_sp.dayofweek ==  5, "cuma")
                    .when(df_sp.dayofweek ==  6, "cumartesi")
                    .when(df_sp.dayofweek ==  7, "pazar")
                    .otherwise("bilinmiyor")
                                         )



aa=df_sp.groupBy("day_name").count().toPandas()
df_sp=px.bar(aa, x="day_name", y="count", title="day_name")





df_sp=df_sp.filter(df_sp["day_name"].isin(['cumartesi', 'pazar']))


df_sp=px.bar(df_sp.groupBy("mount_s").count().toPandas(), x="mount_s" , y="count")

count_t=df_sp.groupBy("payment_type").count().toPandas()

df_sp=px.bar(count_t,x="payment_type" , y="count", title="payment")

count_p=df_sp.groupBy("passenger_count").count().toPandas()

df_sp=px.bar(count_p, x="passenger_count", y="count", title="passenger")

count_s=df_sp.groupBy("store_and_fwd_flag").count().toPandas()
df_sp=px.bar(count_s,x="store_and_fwd_flag", y="count" ,title="strove count" )

tt=df_sp.select('pickup_longitude', 'pickup_latitude','dropoff_longitude' ,'dropoff_latitude').dropna().toPandas()

kep=KeplerGl()
kep.add_data(data=tt, name='konum')



kep.save_to_html(file_name="konumları_gor.html")

df_sp=df_sp.orderBy(df_sp['fare_amount'] , ascending=False)

from google.colab import drive
drive.mount('/content/drive')