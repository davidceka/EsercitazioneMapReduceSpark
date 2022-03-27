from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import explode, col, length, count, max, round, broadcast, lower, split

"""
ATTENZIONE: se script da errori relativi allo spilling dei DataFrame in memoria, aumentare la RAM disponibile nelle impostazioni di spark: $SPARK_HOME/conf/spark_defaults.conf
"""
spark = SparkSession.builder.appName("Spark - Esercitazione Gruppo3").config("spark.some.config.option", "some-value").getOrCreate()

sc = spark.sparkContext
sqlContext = SQLContext(sc)


"""
Lettura utilizzando funzione SparkSession.read per ottenere direttamente un DataFrame senza necessità di filtrare l'header
"""
#testo = spark.read.format("tsv").option("sep", "\t").option("header", "true").csv("/home/dave/EsercitazioneMapReduceSPark/spark/amazon_reviews_us_Video_Games_v1_00.tsv")
testo = spark.read.format("tsv").option("sep", "\t").option("header", "true").csv("hdfs://192.168.104.45:9000/test.tsv")


"""
Select per eliminare le colonne del dataset che non ci interessano e preparare i tipi dei dati nelle colonne agli step successivi
"""
df_testo = testo.select(col("product_title").alias("product_title_key"),
                        split(lower(col("product_title")), ' ').alias("product_title_field"),
                        "star_rating",
                        split(lower(col("review_headline")), ' ').alias("review_headline"),
                        split(lower(col("review_body")), ' ').alias("review_body")
                        ).filter(col("verified_purchase")=='Y')


"""
Conteggio delle occorrenze dei product title e filtraggio dei prodotti che hanno meno di 10 recensioni attraverso un innerJoin
"""
df_occ_pt = df_testo.groupBy("product_title_key").agg(count('*').alias('conteggio_pt')).filter(col('conteggio_pt')>=10)
df_testo = df_testo.join(broadcast(df_occ_pt), ['product_title_key'], 'inner')


"""
Generazione della tabella prodotti in preparazione a query successive
"""
df_testo.createOrReplaceTempView("prodotti")


"""
Settaggio dei DataFrame che non verranno più utilizzati su non persistenti per risparmiare memoria (accelera la computazione se macchina spark ha poca ram, nessun cambiamento altrimenti)
"""
df_occ_pt.unpersist()
df_testo.unpersist()


"""
Explode dei campi product_title, review_headline, review_body con chiave product_title_key per ottenere un dataframe che ad ogni recensione faccia corrispondere le parole del rispettivo
campo della stessa. 
"""
df_product_title = df_testo.select(df_testo.product_title_key, explode(df_testo.product_title_field).alias('parola')).where(length(col('parola'))>=5)
df_review_headline = df_testo.select(df_testo.product_title_key, explode(df_testo.review_headline).alias('parola')).filter(length(col('parola'))>=5)
df_review_body = df_testo.select(df_testo.product_title_key, explode(df_testo.review_body).alias('parola')).filter(length(col('parola'))>=5)


"""
Unione dei campi review_headline e review_body e settaggio ad unpersistent dei DataFrame non più utili
"""
df_review_text = df_review_headline.union(df_review_body)
df_review_headline.unpersist()
df_review_body.unpersist()


"""
Filtraggio delle parole che compaiono nel product_title sull'unione precedente con un leftAntiJoin
"""
df_testo_joined = df_review_text.join(broadcast(df_product_title), ['product_title_key', 'parola'], "leftanti")


"""
Conteggio delle parole su aggregazione in base al product_title
"""
df_testo_contato = df_testo_joined.groupBy('product_title_key', 'parola').agg(count('*').alias('occorrenza')).cache()


"""
Ragruppamento ulteriore per ottenere la parola con occorrenza massima
"""
df_contato_maxed = df_testo_contato.groupBy('product_title_key').agg(max('occorrenza').alias('max_occorrenza'))


"""
Join per recuperare info sulla parola perse a causa del GroupBy precedente e settaggio ad unpersistent
dei DataFrame non più utili
"""
df_contato_maxed = df_contato_maxed.withColumnRenamed('product_title_key', 'new_ptk')
df_contato_joined = df_contato_maxed.join(df_testo_contato, (df_contato_maxed.new_ptk==df_testo_contato.product_title_key)&(df_contato_maxed.max_occorrenza==df_testo_contato.occorrenza), "inner")
df_testo_contato.unpersist()
df_contato_maxed.unpersist()


"""
Select per evidenziare i campi product_title, parola con occorrenza massima e sua occorrenza
"""
parziale=df_contato_joined.select("new_ptk","parola","occorrenza")


"""
Calcolo della media per star rating
"""
tabella_average=spark.sql("SELECT first(conteggio_pt) as occorrenze_product_title,avg(star_rating) as star_rating_medio,product_title_key FROM prodotti group by product_title_key")


"""
Join e sort di tabella_average e parziale al fine di ottenere il DataFrame finale e settaggio ad unpersistent
dei DataFrame non più utili
"""
finale=parziale.join(tabella_average,parziale.new_ptk==tabella_average.product_title_key).select("product_title_key",round("star_rating_medio",1).alias("star_rating_medio"),"occorrenze_product_title","parola","occorrenza").sort(col("star_rating_medio"),col("occorrenze_product_title"), ascending=[0,0])
parziale.unpersist()
tabella_average.unpersist()


"""
Generazione del file di output effettuando il repartition della tabella finale ad un un'unica partizione
in modo da ottenere un singolo file.csv all'interno della cartella Gruppo3
"""
#finale.repartition(1).write.csv("gruppo3", mode='overwrite')
finale.repartition(1).write.csv("/home/amircoli/Andrea_Chiorrini/Gruppo3", mode='overwrite')