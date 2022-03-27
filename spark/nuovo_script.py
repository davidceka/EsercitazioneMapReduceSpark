from itertools import count
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import explode, col, length, count, max, round, broadcast, lower, split

spark = SparkSession.builder.appName("Spark - Esercitazione Gruppo3").config("spark.some.config.option", "some-value").getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)

#testo = spark.read.format("tsv").option("sep", "\t").option("header", "true").csv("/home/dave/EsercitazioneMapReduceSPark/spark/amazon_reviews_us_Video_Games_v1_00.tsv")
testo = spark.read.format("tsv").option("sep", "\t").option("header", "true").csv("hdfs://192.168.104.45:9000/test.tsv")

df_testo = testo.select(col("product_title").alias("product_title_key"),
                        split(lower(col("product_title")), ' ').alias("product_title_field"),
                        "star_rating",
                        split(lower(col("review_headline")), ' ').alias("review_headline"),
                        split(lower(col("review_body")), ' ').alias("review_body")
                        ).filter(col("verified_purchase")=='Y')

df_occ_pt = df_testo.groupBy("product_title_key").agg(count('*').alias('conteggio_pt')).filter(col('conteggio_pt')>=10)

df_testo = df_testo.join(broadcast(df_occ_pt), ['product_title_key'], 'inner')
df_testo.createOrReplaceTempView("prodotti")
df_occ_pt.unpersist()
df_testo.unpersist()

df_product_title = df_testo.select(df_testo.product_title_key, explode(df_testo.product_title_field).alias('parola')).where(length(col('parola'))>=5)
df_review_headline = df_testo.select(df_testo.product_title_key, explode(df_testo.review_headline).alias('parola')).filter(length(col('parola'))>=5)
df_review_body = df_testo.select(df_testo.product_title_key, explode(df_testo.review_body).alias('parola')).filter(length(col('parola'))>=5)

df_review_text = df_review_headline.union(df_review_body)
df_review_headline.unpersist()
df_review_body.unpersist()

df_testo_joined = df_review_text.join(broadcast(df_product_title), ['product_title_key', 'parola'], "leftanti")

df_testo_contato = df_testo_joined.groupBy('product_title_key', 'parola').agg(count('*').alias('occorrenza')).cache()
df_contato_maxed = df_testo_contato.groupBy('product_title_key').agg(max('occorrenza').alias('max_occorrenza'))
df_contato_maxed = df_contato_maxed.withColumnRenamed('product_title_key', 'new_ptk')
df_contato_joined = df_contato_maxed.join(df_testo_contato, (df_contato_maxed.new_ptk==df_testo_contato.product_title_key)&(df_contato_maxed.max_occorrenza==df_testo_contato.occorrenza), "inner")
df_testo_contato.unpersist()
df_contato_maxed.unpersist()

parziale=df_contato_joined.select("new_ptk","parola","occorrenza")
tabella_average=spark.sql("SELECT first(conteggio_pt) as occorrenze_product_title,avg(star_rating) as star_rating_medio,product_title_key FROM prodotti group by product_title_key")
finale=parziale.join(tabella_average,parziale.new_ptk==tabella_average.product_title_key).select("product_title_key",round("star_rating_medio",1).alias("star_rating_medio"),"occorrenze_product_title","parola","occorrenza").sort(col("star_rating_medio"),col("occorrenze_product_title"), ascending=[0,0])
parziale.unpersist()
tabella_average.unpersist()

#finale.repartition(1).write.csv("gruppo3", mode='overwrite')
finale.repartition(1).write.csv("/home/amircoli/Andrea_Chiorrini/Gruppo3", mode='overwrite')
