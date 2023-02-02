# coding=utf-8

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmcount <file>", file=sys.stderr)
        sys.exit(-1)


spark = (SparkSession.builder
            .appName("Quijote_Session")
                .getOrCreate())

# Recuperar el path del fichero
quijote_file = sys.argv[1]

quijote_string = (spark.read
                .text(quijote_file))

# The count method returns the number of rows
print("Contador: ", quijote_string.count())

# El m?todo show muestra por la consola de comandos las n primeras l?neas del documento
# n es el n?mero de l?neas que muestra. Si no tiene valor devuelve las 20 primeras filas
# truncate indica si se corta la l?nea o si se muestra entera
quijote_string.show(n = 2, truncate=False)

# El m?todo head devuelve las n primeras filas
# En texto devuelve una lista con las filas le?das 
print((quijote_string.head(2)))

# El m?todo take hace lo mismo que el m?todo head, pero almacenando los valores en una lista de filas, indicando pues todas
# las variables de dicha fila. 
print(quijote_string.take(2))

# El m?todo first devuelve la primera fila del dataframe, indicanod todas las variables de dicha fila
print(quijote_string.first())

spark.stop()

