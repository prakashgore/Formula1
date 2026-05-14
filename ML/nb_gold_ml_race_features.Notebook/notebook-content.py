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

# CELL ********************

from pyspark.sql import functions as F
results = spark.read.table("silver.fact_race_results").alias("res")
races = spark.read.table("silver.dim_race").alias("rac")
drivers = spark.read.table("silver.dim_driver").alias("drv")
constructors = spark.read.table("silver.dim_constructor").alias("con")
status = spark.read.table("silver.dim_status").alias("sta")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base_df = (
    results
    .join(races,"raceId")
    .join(status,"statusId")
    .select(
        "raceId",
        "season",
        "driverId",
        "constructorId",
        "grid_position",
        "finishing_position",
        "points",
        "status"
    )
)
display(base_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base_df = base_df.withColumn(
    "IS_WINNER",
    F.when(F.col("finishing_position")==1,1)
    .otherwise(0)
    )
display(base_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

driver_stats = (
    base_df
    .groupBy("driverId")
    .agg(
        F.count("*").alias("TOTAL_DRIVER_RACES"),
        F.sum("points").alias("TOTAL_DRIVER_POINTS"),
        F.sum("IS_WINNER").alias("TOTAL_DRIVER_WINS"),
        F.avg("grid_position").cast("int").alias("AVG_DRIVER_GRID_POSITION"),
        F.avg("finishing_position").cast("int").alias("AVG_DRIVER_FINISH_POSITION")
        
    )
)
display(driver_stats)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

constructor_stats = (
    base_df
    .groupBy("constructorId")
    .agg(
        F.count("*").alias("TOTAL_CONSTRUCTOR_RACES"),
        F.sum(
            F.when(F.col("status")=="Finished",1).otherwise(0)).alias("TOTAL_CONSTRUCTOR_FINISHES"),
        F.sum(
            F.when((F.col("status")!="Finished") | (F.col("status").rlike("\\+\\d+ Lap")),1).otherwise(0)).alias("TOTAL_CONSTRUCTOR_DNF")
    )
    .withColumn(
        "CONSTRUCTOR_DNF_RATE",
        (F.col("TOTAL_CONSTRUCTOR_DNF")/F.col("TOTAL_CONSTRUCTOR_RACES"))
    )

)
display(constructor_stats)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

circuit_risk = (
    base_df
    .groupBy("raceId")
    .agg(
        F.avg(
            F.when((F.col("status")!="Finished")|(F.col("status").rlike("\\+\\d+ Lap")),1).otherwise(0)
        ).alias("CIRCUIT_DNF_RATE")
    )
)
display(circuit_risk)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

recent_form = (
    base_df
    .groupBy("driverId","season")
    .agg(
        F.avg("points").alias("RECENT_AVG_POINTS"),
        F.avg("finishing_position").cast("int").alias("RECENT_AVG_FINISH_POSITION")
    )
)
display(recent_form)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ml_features = (
    base_df
    .join(driver_stats,"driverId")
    .join(constructor_stats,"constructorId")
    .join(circuit_risk,"raceId")
    .join(recent_form,["driverId","season"])
    .select(
        F.col("raceId").alias("RACEID"),
        F.col("season").alias("SEASON"),
        F.col("driverId").alias("DRIVERID"),
        F.col("constructorId").alias("CONSTRUCTORID"),
        F.col("grid_position").alias("GRID_POSITION"),
        "TOTAL_DRIVER_RACES",
        "TOTAL_DRIVER_POINTS",
        "TOTAL_DRIVER_WINS",
        "AVG_DRIVER_GRID_POSITION",
        "AVG_DRIVER_FINISH_POSITION",
        "CONSTRUCTOR_DNF_RATE",
        "CIRCUIT_DNF_RATE",
        "RECENT_AVG_POINTS",
        "RECENT_AVG_FINISH_POSITION",
        "IS_WINNER"
    )
)
display(ml_features)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ml_features.write.format("delta").mode("overwrite").option("header","true").saveAsTable("gold.gold_ml_features")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
