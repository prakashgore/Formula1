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

# CELL ********************

def read_csv(path):
    return(
        spark.read
        .option('header','true')
        .option('inferSchema','true')
        .csv(path)
    )    
    
files = mssparkutils.fs.ls("Files/f1_source_data")

component = []
for file in files:
    if file.name.endswith(".csv"):
        component.append((file.name.split(".csv")[0],file.path))
        df = read_csv(file.path)
        # display(f"{file.name}")
display(component)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
def read_csv(path):
   return (
       spark.read
       .option("header", "true")
       .option("inferSchema", "true")
       .csv(path)
   )
files = mssparkutils.fs.ls("Files/f1_source_data")
for file in files:
   if file.name.endswith(".csv"):
       table_name = file.name.replace(".csv", "")
       file_path  = file.path
       print(f"Writing {table_name} to bronze...")
       df = (
           read_csv(file_path)
           .withColumn("ingestion_ts", F.current_timestamp())
           .withColumn("source_file", F.input_file_name())
       )
       (
           df.write
           .format("delta")
           .mode("overwrite")
           .option("mergeSchema", "true")
           .saveAsTable(f"bronze.{table_name}")
       )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
