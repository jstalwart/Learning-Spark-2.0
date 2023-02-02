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

quijote_df = (spark.read
                .text(quijote_file))

spark.stop()

