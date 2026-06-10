import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def main():
    spark = SparkSession.builder \
        .appName("spark_wordcount_df") \
        .getOrCreate()

    ruta_entrada = sys.argv[1]
    ruta_salida = sys.argv[2]

    print(f"Leyendo  desde: {ruta_entrada}")
    print(f"Guardando  en: {ruta_salida}")

    df = spark.read.text(ruta_entrada)

    df_limpio = df.withColumn("texto_limpio", F.regexp_replace(F.lower(F.col("value")), "[^a-z0-9]", " "))

    df_palabras = df_limpio.withColumn("palabra", F.explode(F.split(F.col("texto_limpio"), "\\s+")))

    stopwords = ["the", "and", "a", "to", "of", "in", "i", "is", "that", "it", "on", "you", "this", "for", "but", "with", "are", "have", "be", "at", "or", "as", "was", "so", "if", "out", "not", "from", "we", "they", "by", "an"]
    
    df_filtrado = df_palabras.filter(
        (~F.col("palabra").isin(stopwords)) & 
        (F.length(F.col("palabra")) > 2)
    )

    conteos_df = df_filtrado.groupBy("palabra").count()

    resultado = conteos_df.select(F.concat_ws("\t", F.col("palabra"), F.col("count").cast("string")))

    resultado.write.text(ruta_salida)

    print("WordCount terminado")
    spark.stop()

if __name__ == "__main__":
    main()
