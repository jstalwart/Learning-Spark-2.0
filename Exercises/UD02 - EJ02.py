# coding=utf-8

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import max

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmmax <file>", file=sys.stderr)
        sys.exit(-1)

spark = (SparkSession.builder
            .appName("Mnms_Exercise")
            .getOrCreate())

mnm_file = sys.argv[1]

mnm_df = (spark.read
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(mnm_file)

spark.stop()