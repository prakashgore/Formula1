# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "81f0def5-3a75-46fc-b336-be49236ef15d",
# META       "default_lakehouse_name": "raw",
# META       "default_lakehouse_workspace_id": "09a0eef0-8957-4dac-9acd-3af916afce13",
# META       "known_lakehouses": [
# META         {
# META           "id": "81f0def5-3a75-46fc-b336-be49236ef15d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # **Raw to bronze incremental load**

# MARKDOWN ********************

# **This notebook enables the Incremental Data Loading feature into our project. New and Updated data is Dumped in the Raw Lakehouse itself, Races and Results data is updated from weekend-to-weekend.
# In 'Raw/Files/f1_source_data' we have races and results subfolders to dump new data that is to be appended.**

# MARKDOWN ********************

# - **Reading the CSV files for races and results from their dump folder respectively.**
# - **Adding a time stamp with input file name for future proof of appending file and time of appending.**

# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_race = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("Files/f1_source_data/races/")
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
)

raw_results = (
   spark.read
       .option("header", "true")
       .option("inferSchema", "true")
       .csv("Files/f1_source_data/results/")
       .withColumn("ingestion_ts", F.current_timestamp())
       .withColumn("source_file", F.input_file_name())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import to_date, col
raw_race = (
   raw_race
   .withColumn("date", to_date(col("date"), "dd-MM-yyyy"))
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# - **This block of code ensures that only unique values are merged to bronze.races & bronze.results.**
# - **Avoids duplication of race and result data**

# CELL ********************

from delta.tables import DeltaTable
bronze_results = DeltaTable.forName(spark,"bronze.results")
bronze_races = DeltaTable.forName(spark,"bronze.races")
(
    bronze_races.alias("b_races")
    .merge(
        raw_race.alias("r_races"),
        "b_races.raceId = r_races.raceId"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

(
    bronze_results.alias("b_results")
    .merge(
        raw_results.alias("r_results"),
        "b_results.resultId = r_results.resultId"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
