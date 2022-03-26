from itertools import count
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col, length, count, max
from pyspark.sql.types import Row


spark = SparkSession \
.builder \
.appName("Python Spark SQL basic example") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()

sc = spark.sparkContext
#testo_con_header = sc.textFile("/home/andrea/Desktop/ChallengeBDA/dataset/amazon_reviews_us_Video_Games_v1_00.tsv",48)
testo = sc.textFile("/home/dave/EsercitazioneMapReduceSPark/spark/amazon_reviews_us_Video_Games_v1_00.tsv", 48)
header = testo.first()
testo_no_headline = testo.map(lambda l: l.split("\t")).filter(lambda line: (line!=header and line[11]=="Y"))
testo_rdd = testo_no_headline.map(lambda p: Row(product_title_key=p[5],
                                                    product_title_field=p[5].lower().split(' '),
                                                    star_rating=int(p[7]),
                                                    review_headline=p[12].lower().split(' '),
                                                    review_body=p[13].lower().split(' ')))

df_testo = spark.createDataFrame(testo_rdd)

df_occ_pt = df_testo.groupBy("product_title_key").agg(count('*').alias('conteggio_pt')).filter(col('conteggio_pt')>=10)

df_testo = df_occ_pt.join(df_testo, ['product_title_key'], 'inner')

df_product_title = df_testo.select(df_testo.product_title_key, explode(df_testo.product_title_field).alias('parola')).where(length(col('parola'))>=5)
df_review_headline = df_testo.select(df_testo.product_title_key, explode(df_testo.review_headline).alias('parola')).filter(length(col('parola'))>=5)
df_review_body = df_testo.select(df_testo.product_title_key, explode(df_testo.review_body).alias('parola')).filter(length(col('parola'))>=5)

df_review_headline_filtered = df_review_headline.join(df_product_title, ['product_title_key', 'parola'], "leftanti")
df_review_body_filtered = df_review_body.join(df_product_title, ['product_title_key', 'parola'], "leftanti")

df_testo_joined = df_review_body_filtered.union(df_review_headline_filtered)

df_testo_contato = df_testo_joined.groupBy('product_title_key', 'parola').agg(count('*').alias('occorrenza')).cache()
#df_testo_contato = df_testo_joined.groupBy('product_title_key', 'parola').agg(count('*').alias('conteggio'),max('parola').alias('parola_piu_frequente'))
df_contato_maxed = df_testo_contato.groupBy('product_title_key').agg(max('occorrenza').alias('max_occorrenza'))
df_contato_joined = df_contato_maxed.join(df_testo_contato, (df_contato_maxed.product_title_key==df_testo_contato.product_title_key)&(df_contato_maxed.max_occorrenza==df_testo_contato.occorrenza), "inner")

# df_prova = df_testo_joined.withColumn('count', col('count')).groupBy('parola','product_title_key').agg({'count': 'count'}).select('product_title_key', 'parola', 'count')

# select product_title_key,parola,count(parola) as conteggio
# from tabella
# group by parola,product_title_key
# order by product_title_key