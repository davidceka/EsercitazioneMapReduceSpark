from pyspark.sql import SparkSession
import pyspark.sql as pysql
from pyspark.sql.types import Row

def func(line):
    review_headline = line[12].lower().split(" ")
    review_body = line[13].lower().split(" ")
    product_title = line[5].lower().split(" ")

    parole = []

    for parola in review_headline:
        if len(parola)>=5 and parola not in review_body:
            if parola not in product_title:
                parole.append(parola)
    for parola in review_body:
        if len(parola)>=5 and parola not in product_title:
            parole.append(parola)
    
    return line[5], parole

def func2(listaparole1, listaparole2):
   listafinale = []
   listafinale.extend(listaparole1)
   listafinale.extend(listaparole2)
   return listafinale

def func3(elemento):
    lista_parole = elemento[1]
    piu_freq = max(set(lista_parole), key= lista_parole.count)
    quante_volte = lista_parole.count(piu_freq)

    return elemento[0], piu_freq, quante_volte
    
spark = SparkSession \
.builder \
.appName("Python Spark SQL basic example") \
.config("spark.some.config.option", "some-value") \
.getOrCreate()

sc = spark.sparkContext
#testo_con_header = sc.textFile("/home/andrea/Desktop/ChallengeBDA/dataset/amazon_reviews_us_Video_Games_v1_00.tsv",48)
testo_con_header = sc.textFile("/home/dave/EsercitazioneMapReduceSPark/spark/amazon_reviews_us_Video_Games_v1_00.tsv", 48)
header = testo_con_header.first()
testo = testo_con_header.filter(lambda line: line!=header)

parts = testo.map(lambda l: l.split("\t"))
valid_parts = parts.filter(lambda y:y[11]=="Y")
#prodotti = valid_parts.map(lambda p: Row(product_title=p[5],star_rating=int(p[7]),review=p[13]))
prodotti = valid_parts.map(lambda p: Row(product_title=p[5],star_rating=int(p[7])))


df_prodotti = spark.createDataFrame(prodotti)

prova=valid_parts.map(lambda line: func(line))
prova2 = prova.reduceByKey(func2)
prova3 = prova2.map(lambda e: func3(e)).map(lambda p: Row(product_title=p[0], max_parola=p[1], max_occ=p[2]))


df_prova = spark.createDataFrame(prova3)






#"product_title" , avg(star_rating), occorrenze("product_title"), parola+frequente, conteggio parola+frequent