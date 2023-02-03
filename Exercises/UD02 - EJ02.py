# coding=utf-8

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count, max, avg, min

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: mnmmax <file>", file=sys.stderr)
        sys.exit(-1)

var = False
while not var:
    var = (input("Select the number of the function you will be applying: \n  1. Count.\n  2. Average.\n  3. Maximum.\n  4. Minimum.\n"))
    if var not in ["1", "2", "3", "4"]:
        print("Wrong variable assignation. The input variable must be a number ranged from 1 to 4 (both included).")
        var = False

var_names = {"1" : "Total Count", 
             "2" : "Average", 
             "3" : "Maximum", 
             "4" : "Minimum"}

spark = (SparkSession.builder
            .appName("Mnms_Exercise")
            .getOrCreate())

mnm_file = sys.argv[1]

# Read df
mnm_df = (spark.read
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(mnm_file))

#Max function
mnm_df_2 = (mnm_df.select("State", "Color", "Count")
                    .where((mnm_df.State == "CA") | (mnm_df.State == "TX"))
                    .groupBy("State", "Color"))

if var == "1":
    mod_mnm_df = mnm_df_2.agg(count("Count").alias(var_names[var]))
elif var == "2":
    mod_mnm_df = mnm_df_2.agg(avg("Count").alias(var_names[var]))
elif var == "3":
    mod_mnm_df = mnm_df_2.agg(max("Count").alias(var_names[var]))
else:
    mod_mnm_df = mnm_df_2.agg(min("Count").alias(var_names[var]))

def_mnm_df = mod_mnm_df.orderBy(var_names[var], ascending = False)

def_mnm_df.show(def_mnm_df.count(), truncate=False)

spark.stop()