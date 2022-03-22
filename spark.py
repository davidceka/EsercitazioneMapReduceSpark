from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession


def script():
    sc = SparkContext.getOrCreate()
    spark = SparkSession(sc)
    testo_con_header = sc.textFile("/home/andrea/Desktop/ChallengeBDA/dataset/amazon_reviews_us_Video_Games_v1_00.tsv") #Test in locale

    # product_title --- MEAN_star_rating --- occorrenze(product_title) --- word(>=5char, da headline e body) --- occorrenze(word - word in product_title)

    header = testo_con_header.first()
    testo = testo_con_header.filter(lambda line: line!=header)

    rec_valide = testo.filter(lambda line: line.split[11]=="Y")

    



    lista_risultato = []

    with open('/home/amircoli/Andrea_Chiorrini/gruppo3.csv', 'w') as filehandle:
        for riga in lista_risultato.collect():
            filehandle.write(str(riga[0])+","+str(riga[1])+.... str(...)+"\n")

if __name__ == "__main__":
    script()