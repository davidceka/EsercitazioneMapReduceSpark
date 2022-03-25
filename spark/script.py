from subprocess import STARTF_USESHOWWINDOW
from pyspark.sql import SparkSession
import pyspark.sql as pysql
from pyspark.sql.types import Row

def func(line):
    review_headline = line[12].split(" ")
    review_body = list(line[13].split(" "))
    product_title = line[5].split(" ")

    parole = []

    for parola in review_headline:
        if parola not in review_body:
            if parola not in product_title:
                parole.append(parola)
    
    return line[5], parole

def func2(primo, secondo):
    product_title1 = primo[0]
    list_parole1 = primo[-1]

    àstesso per secondo

    if product è lo stesso

    return parola con piu occ




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
#prodotti = valid_parts.map(lambda p: Row(product_title=p[5],star_rating=int(p[7]),review=p[13]))
prodotti = valid_parts.map(lambda p: Row(product_title=p[5],star_rating=int(p[7])))


df_prodotti = spark.createDataFrame(prodotti)

prova=valid_parts.map(lambda line: func(line))
prova2 = prova.reduce(lambda primo, secondo: func2(primo, secondo))







#"product_title" , avg(star_rating), occorrenze("product_title"), parola+frequente, conteggio parola+frequent