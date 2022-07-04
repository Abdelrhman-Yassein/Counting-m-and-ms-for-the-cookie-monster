# import necessary libraries
# From Pyspark module
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage : mnmcount <file>", file=sys.stderr)
        sys.exit(-1)
    # build s SparkSession APIs.
    spark = (SparkSession
             .builder
             .appName("PythonMnMCount")
             .getOrCreate())

    # Get the M & M data set filename from the command-line argumeents
    mnm_file = sys.argv[1]

    # Read the file into spark dataFrame Using The CSV formate
    mnm_df = (spark
              .read
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(mnm_file))
    # use the DataFrame high-level APIs. Note
    count_mnm_df = (mnm_df
                    .select("State", "Color", "Count")
                    .groupBy("State", "Color")
                    .agg(count("Count").alias("Total"))
                    .orderBy("Total", ascending=False))

    # show the resulting aggregations for all the states and colors
    count_mnm_df.show(n=60, truncate=False)
    print(f"Total Rows = {count_mnm_df.count()}")

    # Find the aggreate count for California by Filtering
    ca_count_mnm_df = (mnm_df
                       .select("Color", "State", "Count")
                       .where(mnm_df.State == "CA")
                       .groupBy("Color", "State")
                       .agg(count("Count").alias("Total"))
                       .orderBy("Total", ascending=False))

    # show the resulting aggregation for California
    ca_count_mnm_df.show(n=10, truncate=False)
    print(f"Total Rows For California {ca_count_mnm_df.count()}")
