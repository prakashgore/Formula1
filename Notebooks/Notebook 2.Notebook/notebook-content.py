# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6656573f-166e-4e43-bee6-6156a4247f67",
# META       "default_lakehouse_name": "bronze",
# META       "default_lakehouse_workspace_id": "09a0eef0-8957-4dac-9acd-3af916afce13",
# META       "known_lakehouses": [
# META         {
# META           "id": "6656573f-166e-4e43-bee6-6156a4247f67"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
drivers = spark.read.table("bronze.drivers")
drivers = drivers.withColumn(
   "driver_personal_email",
   F.when(
       (F.col("number").isNotNull()) & (F.col("number") != "\\N"),
       F.concat(
           F.lower(F.col("forename")),
           F.lower(F.col("surname")),
           F.col("number"),
           F.lit("@gmail.com")
       )
   ).otherwise(None)
)
display(drivers)     
drivers.write.format('delta').mode('overwrite').option('header', 'true').option('mergeSchema','true').saveAsTable('bronze.drivers')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  

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
