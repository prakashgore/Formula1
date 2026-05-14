# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4f6143db-b38a-4498-a3a9-88631a4d943c",
# META       "default_lakehouse_name": "silver",
# META       "default_lakehouse_workspace_id": "09a0eef0-8957-4dac-9acd-3af916afce13",
# META       "known_lakehouses": [
# META         {
# META           "id": "4f6143db-b38a-4498-a3a9-88631a4d943c"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # **Bronze to Silver Dimension and Fact Table Modeling**

# MARKDOWN ********************

# **This Notebook creates Dimensions and Fact tables in Silver Lakehouse**
# ### 1. **Dimension Tables**
# - **dim_driver**
# - **dim_constructor**
# - **dim_circuit**
# - **dim_race**
# - **dim_status**
# ### 2. **Fact Tables**
# - **fact_race_results**
# - **fact_lap_times**
# - **fact_qualifying**

# MARKDOWN ********************

# ## Importing Libraries and necessary functions

# CELL ********************

from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql import functions as F


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Creating dim tables

# CELL ********************

dim_driver = (spark.read.table("bronze.drivers")

.withColumn("driver_name", F.concat_ws(" ", F.col("forename"),F.col("surname")))
.drop("forename","surname")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_constructor = (
    spark.read.table("bronze.constructor")
    .withColumnRenamed("name","constructor_name")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_circuit = (
    spark.read.table('bronze.circuits')
    .withColumnRenamed("name","circuit_name")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_race = (
    spark.read.table("bronze.races")
    .filter(F.col("year")>=2010)
    .withColumnRenamed("year", "season")

)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_status = (spark.read.table('bronze.status'))
dim_status.write.format('delta').mode('overwrite').option('header','true').option("overwriteSchema","true").saveAsTable('dim_status')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_driver.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('dim_driver')
dim_constructor.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('dim_constructor')
dim_circuit.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('dim_circuit')
dim_race.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('dim_race')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_race_results = (
    spark.read.table("bronze.results")
    .join(dim_driver,"driverId")
    .join(dim_constructor,"constructorId")
    .join(dim_race,"raceId")
    .withColumnRenamed("grid","grid_position")
    .withColumnRenamed("positionOrder","finishing_position")
    .select(
        "raceId"
        ,"circuitId"
        ,"constructorId"
        ,"driverId"
        ,"resultId"
        ,"statusId"
        ,"grid_position"
        ,"finishing_position"
        ,"points"
        ,"laps"
        ,"milliseconds"
        ,"fastestLap"
        ,"rank"
        ,"fastestLapTime"
        ,"fastestLapSpeed"
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


fact_lap_times = (
    spark.read.table("bronze.lap_times")
    .join(dim_driver,"driverId")
    .join(dim_race,"raceId")
    .select(
        "raceId"
        ,"driverId"
        ,"circuitId"
        ,"lap"
        ,"milliseconds"
    )
    .withColumnRenamed("lap","lap_number")
    .withColumnRenamed("milliseconds","lap_time_ms")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


fact_qualifying = (spark.read.table("bronze.qualifying")
.join(dim_driver,"driverId")
.join(dim_constructor,"constructorId")
.join(dim_race,"raceId")
.select(
    "raceId"
    ,"driverId"
    ,"constructorId"
    ,"position"
    ,F.col("q1")
    ,"q2"
    ,"q3"
)
    .withColumnRenamed("position","qualifying_position")    
    .fillna({"q2":"Out of Time","q3":"Out of Time"})

)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_race_results.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('fact_race_results')
fact_lap_times.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('fact_lap_times')
fact_qualifying.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('fact_qualifying')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
