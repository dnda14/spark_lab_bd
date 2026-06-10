import sys
from pyspark.sql import SparkSession
import re

 
def main():
    spark = SparkSession.builder\
        .appName("spark_indice_invertido")\
        .getOrCreate()

    ruta_entrada = sys.argv[1]
    ruta_salida = sys.argv[2]

    print(f"Leyendo  desde: {ruta_entrada}")
    print(f"Guardando  en: {ruta_salida}")

    linea = spark.sparkContext.textFile(ruta_entrada)
    min_pal = {"the", "and", "a", "to", "of", "in", "i", "is", "that", "it", "on", "you", "this", "for", "but", "with", "are", "have", "be", "at", "or", "as", "was", "so", "if", "out", "not", "from", "we", "they", "by", "an"}

    def extraer(linea):
        par_fuente_texto = linea.split(",",1)
        if len(par_fuente_texto) == 2:
            fuente = par_fuente_texto[0]
            texto = par_fuente_texto[1]
            texto = texto.lower()
            pals = re.findall(r'[a-zA-Z0-9]+', texto)

            par = []
            for pal in pals:
                if pal not in min_pal and len(pal) > 2:
                    par.append((pal,fuente))
            return par
        return []
    
    pares = linea.flatMap(extraer).map(lambda x: (x[0], {x[1]}))
    
    indice = pares.reduceByKey(lambda a, b: a | b)
    
    indice_final = indice.mapValues(list)
    
    indice_final.saveAsTextFile(ruta_salida)
    spark.stop()



if __name__ == '__main__':
    main()


        



    
