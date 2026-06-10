import sys
import re
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Spark_WordCount") \
        .getOrCreate()

    if len(sys.argv) != 3:
        print("Uso: spark_wordcount.py <ruta_entrada_s3> <ruta_salida_s3>")
        sys.exit(-1)

    ruta_entrada = sys.argv[1]
    ruta_salida = sys.argv[2]

    print(f"Leyendo  desde: {ruta_entrada}")
    print(f"Guardando  en: {ruta_salida}")

    lineas = spark.sparkContext.textFile(ruta_entrada)

    pals = lineas.flatMap(lambda linea: re.findall(r'[a-zA-Z0-9]+', linea.lower()))
    
    pares = pals.map(lambda palabra: (palabra, 1))
    
    conteos = pares.reduceByKey(lambda a, b: a + b)

    conteos.saveAsTextFile(ruta_salida)

    print("Proceso terminado")
    
    spark.stop()

if __name__ == "__main__":
    main()
