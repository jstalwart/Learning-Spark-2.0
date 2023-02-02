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

print("Contador: ", quijote_string.count())

# El m�todo show muestra por la consola de comandos las n primeras l�neas del documento
# n es el n�mero de l�neas que muestra
# truncate indica si se corta la l�nea o si se muestra entera
quijote_string.show(n=2, truncate=False)



spark.stop()

