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

# Contador
print("Contador:", quijote_string.count())

spark.stop()

