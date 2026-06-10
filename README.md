# Apache Spark en AWS EMR

El objetivo principal del proyecto es realizar la implementacion y un analisis de rendimiento de WordCount e Indice Invertido en Spark.

##  Tecnologías Utilizadas

- **AWS EMR (Elastic MapReduce)**: Creación y gestión del clúster en la nube.
- **Amazon S3**: Almacenamiento distribuido para los datasets de entrada y resultados.
- **Apache Spark**: Procesamiento en memoria de datos masivos usando la API de DataFrames (`pyspark`).
- **Python (Boto3)**: Scripts para la automatización de infraestructura, envío de trabajos y monitoreo.

##  Estructura del Proyecto

### 1. Automatización de Infraestructura (AWS Boto3)
- `levantar_emr.py`: Script para aprovisionar y configurar el clúster de AWS EMR de manera automática.
- `lanzar_step1.py` / `lanzar_step2.py`: Scripts para enviar trabajos (steps) al clúster EMR una vez que está en ejecución.
- `preparar_experimentos.py`: Preparación de los datos y configuración necesaria en S3 antes de la ejecución.
- `ver_metricas.py`: Herramienta para extraer y analizar las métricas de tiempo y rendimiento de los trabajos ejecutados.

### 2. Trabajos de Procesamiento (Apache Spark)
- `spark_wordcount_df.py`: Implementación del clásico algoritmo de *WordCount* utilizando DataFrames de Spark.
- `spark_indice_df.py`: Implementación de un *Índice Invertido* (Inverted Index) utilizando DataFrames de Spark.

