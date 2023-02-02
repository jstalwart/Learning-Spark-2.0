# coding=utf-8

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)

# Construimos una sesión spark (creándola o recuperándola) con nombre Python_MnM_count
spark = (SparkSession.builder
            .appName("Python_MnM_count")
                .getOrCreate())

# Recuperar el fichero desde la línea de argumentos del shell
mnm_file = sys.argv[1]

# Cargamos los datos de M&M. Aquí tenemos varias opciones: consideramos el header, 
mnm_df = (spark.read
            .format("csv")
                .option("header", "true")
                    .option("inferSchema", "true")
                        .load(mnm_file))

# Contamos las filas del dataframe de mnms. 
# 1. Hacemos un select de las columnas que nos interesan. 
# 2. Agrupamos el dataframe según las columnas de interés. 
# 3. Sumamos todas las columnas Count por grupo y guardamos la variable como 'Total'. 
# 4. Ordenamos los resultados según la nueva variable
count_mnm_df = (mnm_df.select("State", "Color", "Count")
                    .groupBy("State", "Color")
                        .agg(count("Count").alias("Total"))
                             .orderBy("Total", ascending = False))

# Finalmente mostramos este conteo para todos los estados y colores
count_mnm_df.show(n=60, truncate=False)
print("Total rows = %d" % (count_mnm_df.count()))

# Seguimos contando solamente para California. 
# 1. Selecionamos todas las filas del dataset. 2. Seleccionamos aquellas filas del dataset donde la columna 'State' sea 'CA'

ca_count_mnm_df = (mnm_df.select("State", "Color", "Count")
                        .where(mnm_df.State == "CA")
                            .groupBy("State", "Color")
                                .agg(count("Count").alias("Total"))
                                    .orderBy("Total", ascending = False))

ca_count_mnm_df.show(n=10, truncate=False)

# Detenemos la sesión Spark
spark.stop()
                        