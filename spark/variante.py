from pyspark.sql import SparkSession
import pyspark.sql as pysql
from pyspark.sql.types import Row
import pyspark.sql.functions as f




spark = SparkSession \
.builder \
.appName("Python Spark SQL basic example") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()

sc = spark.sparkContext
testo_con_header = sc.textFile("/home/andrea/Desktop/ChallengeBDA/dataset/amazon_reviews_us_Video_Games_v1_00.tsv",48)
#testo_con_header = sc.textFile("/home/andrea/Desktop/ChallengeBDA/dataset/amazon_reviews_us_Books_v1_02.tsv")
header = testo_con_header.first()
testo = testo_con_header.filter(lambda line: line!=header)

parts = testo.map(lambda l: l.split("\t"))
valid_parts = parts.filter(lambda y:y[11]=="Y")
prodotti = valid_parts.map(lambda p: Row(product_title=p[5],star_rating=int(p[7]),review=p[13]))


df_prodotti = spark.createDataFrame(prodotti)


#df.withColumn('word', f.explode(f.split(f.col('review'), ' '))).groupBy('word').count().sort('count', ascending=False).show()

#prova=testo.map(lambda line: (line.split("\t")[13]))
#prova2=prova.flatMap(lambda line:line.split(" "))
#prova3=prova2.map(lambda a:(a,1))





#"product_title" , avg(star_rating), occorrenze("product_title"), parola+frequente, conteggio parola+frequent