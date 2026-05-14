# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b70f457f-ea59-45f3-91ab-3fc6d8e5fc8d",
# META       "default_lakehouse_name": "gold",
# META       "default_lakehouse_workspace_id": "09a0eef0-8957-4dac-9acd-3af916afce13",
# META       "known_lakehouses": [
# META         {
# META           "id": "b70f457f-ea59-45f3-91ab-3fc6d8e5fc8d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.functions import monotonically_increasing_id

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fact_results = spark.read.table("silver.fact_race_results")
dim_driver = spark.read.table("silver.dim_driver")
dim_race = spark.read.table("silver.dim_race")
driver_season_df = (
   fact_results
   .join(dim_driver, "driverId", "inner")
   .join(dim_race, "raceId", "inner")
   .filter(F.col("season")>= 2010)
)
driver_season_profile = (
   driver_season_df
   .groupBy(
       "driverId",
       "driver_name",
       "nationality",
       "season"
   )
   .agg(
       F.count("*").alias("races"),
       F.sum(F.when(F.col("finishing_position") == 1, 1).otherwise(0)).alias("wins"),
       F.sum(F.when(F.col("finishing_position") <= 3, 1).otherwise(0)).alias("podiums"),
       F.sum("points").alias("total_points"),
       F.round(F.avg("grid_position")).cast("int").alias("avg_grid_position"),
       F.round(F.avg("finishing_position")).cast("int").alias("avg_finish_position")
   )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(driver_season_profile)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

driver_season_profile.write.format('delta').mode('overwrite').option("overwriteSchema","true").saveAsTable("driver_season_profile")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window

results  = spark.read.table('silver.fact_race_results').alias('res')
races    = spark.read.table('silver.dim_race').alias('ra')
circuits = spark.read.table('silver.dim_circuit').alias('ci')
drivers  = spark.read.table('silver.dim_driver').alias('dr')

gold_driver_circuit_performance = (
    results
    .filter(
        (F.col("finishing_position")==1) )
    .join(races, F.col("res.raceId")==F.col("ra.raceId"),"inner")
    .join(drivers,F.col("res.driverId")== F.col("dr.driverId"),"inner")
    .join(circuits,F.col("res.circuitId")== F.col("ci.circuitId"),"inner")
    .groupBy(
        "res.driverId",
        "res.circuitId",
        "ci.circuit_name"

    )
    .agg(
        F.count("*").alias("wins_at_circuit"),
        F.avg("fastestLapSpeed").cast('int').alias("avg_fastest_lap_speed")
    )
        
)

window_spec = Window.partitionBy('res.driverId').orderBy(F.desc("wins_at_circuit"))

driver_top_circuit = (
    gold_driver_circuit_performance
    .withColumn("circuit_rank",F.row_number().over(window_spec))
    .filter(F.col("circuit_rank")<=5)
)
display(driver_top_circuit)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

driver_top_circuit.write.format('delta').mode('overwrite').option('header','true').saveAsTable('gold_driver_top_circuits')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

constructor_assets = (spark.read.table("silver.dim_constructor")
.select("constructor_key",
        "constructorId",
        "constructor_name"
        )
)
display(constructor_assets)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option('header','true').csv('Files/constructor_assets/constructor_assets.csv')
df.write.format('delta').option('header','true').mode('overwrite').saveAsTable('constructor_assets')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import avg, sum, count, when
results = spark.read.table('silver.fact_race_results')
races = spark.read.table('silver.dim_race')

constructor_season= (
    results
    .join(races,"raceId")
    .filter(F.col("season")>=2010)
    .groupBy("constructorId","season")
    .agg(
        count("*").alias("races"),
        sum(when(F.col("finishing_position")==1,1).otherwise(0)).alias("wins"),
        sum(when(F.col("finishing_position")<=3,1).otherwise(0)).alias("podiums"),
        avg("grid_position").cast('int').alias("avg_grid_position"),
        avg("finishing_position").cast('int').alias("avg_finish_position")
    )
    .select(
        "constructorId",
        "season",
        "races",
        "wins",
        "podiums",
        "avg_grid_position",
        "avg_finish_position"
    )
    
)
display(constructor_season)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_status = spark.read.table("silver.dim_status")
constructor_race_status = (
    results
    .join(races,"raceId")
    .join(dim_status,"statusId")
    .filter(F.col('season')>=2010)
    .groupBy("constructorId","raceId","season")
    .agg(
        sum(when(F.col("status")=="Finished",1).otherwise(0)).alias("finish_count"),
        sum(when(F.col("status")!="Finished",1).otherwise(0)).alias("dnf_count")
    )
)
display(constructor_race_status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

excluded_statuses = [
   "+1 Lap", "+2 Laps", "+3 Laps",
   "+4 Laps", "+5 Laps","+6 Laps", 
   "+7 Laps", "+8 Laps", "+9 Laps", 
   "+10 Laps", "+11 Laps","Collision",
   "Accident"
    ]
    
constructor_driver_dnf = (
    results
    .join(races,"raceId")
    .join(dim_status,"statusId")
    .filter(F.col("season")>=2010)
    .filter(F.col("status")!='Finished')
    .filter(~F.col("status").isin(excluded_statuses))
    .select(
        "constructorId",
        "raceId",
        "driverId",
        "season",
        "statusId",
        "status"
    )
)
display(constructor_driver_dnf)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
results = spark.read.table("silver.fact_race_results").alias("res")
races   = spark.read.table("silver.dim_race").alias("rac")
status  = spark.read.table("silver.dim_status").alias("sta")

excluded_statuses = [
   "+1 Lap", "+2 Laps", "+3 Laps", 
   "+4 Laps", "+5 Laps","+6 Laps", 
   "+7 Laps", "+8 Laps", "+9 Laps", 
   "+10 Laps", "+11 Laps"
   ]

gold_constructor_race_status = (
   results
   .join(races, F.col("res.raceId") == F.col("rac.raceId"))
   .join(status, F.col("res.statusId") == F.col("sta.statusId"))
   .filter(F.col("rac.season") >= 2010)
   .filter(~F.col("sta.status").isin(excluded_statuses))
   .select(
       # KEYS
       "driverId",
       F.col("res.constructorId").alias("constructor_key"),
       F.col("res.raceId").alias("race_key"),
       F.col("rac.season").alias("season"),
       F.col("res.statusId").alias("status_key"),
       # FLAGS (numeric only)
       F.when(F.col("sta.status") == "Finished", 1).otherwise(0).alias("finish_flag"),
       F.when(F.col("sta.status") != "Finished", 1).otherwise(0).alias("dnf_flag")
   )
)
display(gold_constructor_race_status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

constructor_race_status.write.format('delta').mode('overwrite').option("overwriteSchema","true").saveAsTable("gold_constructor_race_status")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

constructor_driver_dnf.write.format('delta').mode('overwrite').option("header","true").saveAsTable("gold_constructor_driver_dnf")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

constructor_season.write.format('delta').mode('overwrite').saveAsTable("constructor_season")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


dim_season = (spark.read.table('silver.races')
.select(F.col("year").alias("season"))
.distinct()
.filter(F.col("year")>=2010)
.withColumn("season_key",F.col("season"))
)
display(dim_season)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dim_season.write.mode('overwrite').format('delta').saveAsTable("gold_dim_season")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

circuit_assets = (spark.read.table('silver.dim_circuit')
                .select("circuit_key","circuitId","circuit_name","country")
)
display(circuit_assets)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

circuit_assets.coalesce(1).write.mode('overwrite').option('header','true').csv('Files/circuit_assets')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, when 
circuits = spark.read.table('silver.dim_circuit')
gold_dim_circuit = (
    circuits
    .select(
    'circuitId',
    'circuit_name',
    'location',
    'country',
    'lat',
    'lng'
)
)
gold_dim_circuit.write.format('delta').mode('overwrite').saveAsTable('gold_dim_circuit')
display(gold_dim_circuit)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import avg,sum,col,when, count
results = spark.read.table('silver.results')
races = spark.read.table('silver.races')
status = spark.read.table('silver.status')

gold_fact_circuit_race_profile = (
    results
    .join(races,'raceId')
    .join(status,'statusId')
    .filter(col('year')>= 2010)
    .filter(col("status")!='+1 Lap')
    .filter(col("status")!='+2 Laps')
    .filter(col("status")!='+11 Laps')
    .filter(col("status")!='+3 Laps')
    .filter(col("status")!='+4 Laps')
    .filter(col("status")!='+5 Laps')
    .filter(col("status")!='+6 Laps')
    .filter(col("status")!='+7 Laps')
    .filter(col("status")!='+8 Laps')
    .filter(col("status")!='+8 Laps')
    .groupBy('circuitId','year')
    .agg(
        count("*").alias('race_entries'),
        sum(when(col('position')==1,1).otherwise(0)).alias("wins"),
        avg(when(col('position')==1,col('grid'))).alias('avg_grid_pos_winner'),
        avg(col('grid')-col('position')).alias('avg_positions_gained'),
        sum(when(col('status')!='Finished',1).otherwise(0)).alias("total_dnfs"),
        sum(when(col('status')=='Accident',1).otherwise(0)).alias("accident_dnfs"),
        sum(when(col('status').isin('Engine','Gearbox','Hydraulics','Transmission','Electrical','Brakes'),1).otherwise(0)).alias('mechanical_dnfs')
        
    )
    .select(
       "circuitId",
       col("year").alias("season"),
       "race_entries",
       "wins",
       col("avg_grid_pos_winner").cast("int"),
       col("avg_positions_gained").cast('int'),
       "total_dnfs",
       "accident_dnfs",
       "mechanical_dnfs"
   )
)

display(gold_fact_circuit_race_profile)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_fact_circuit_race_profile.write.format('delta').mode('overwrite').saveAsTable('gold_fact_circuit_race_profile')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_circuit_overtaking = (
    results
    .join(races,'raceId')
    .filter(col('year')>=2010)
    .groupBy('circuitId','year')
    .agg(
        avg(col('grid')-col('position')).alias('avg_positions_gained'),
        sum(when((col('grid')-col('position'))>=5,1).otherwise(0)).alias('big_overtakes')
    )
    .select(
        'circuitId',
        col('year').alias('season'),
        col('avg_positions_gained').cast('int'),
        'big_overtakes'

    )
)
display(gold_circuit_overtaking)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_circuit_overtaking.write.format('delta').mode('overwrite').saveAsTable('gold_circuit_overtaking')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_fact_circuit_strategy_index = (
   gold_fact_circuit_race_profile
   .groupBy("circuitId")
   .agg(
       avg("avg_positions_gained").cast('int').alias("overtaking_index"),
       avg("total_dnfs").cast('int').alias("dnf_risk_index"),
       avg("avg_grid_pos_winner").cast('int').alias("qualifying_dependency")
   )
   .withColumn(
       "strategy_type",
       when(col("dnf_risk_index") > 5, "High Risk")
       .when(col("overtaking_index") > 2, "Race Craft Circuit")
       .otherwise("Qualifying Circuit")
   )
)
display(gold_fact_circuit_strategy_index)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_fact_circuit_strategy_index.write.format('delta').mode('overwrite').saveAsTable('gold_fact_circuit_strategy_index')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
drivers = spark.read.table("silver.dim_driver")
results = spark.read.table("silver.fact_race_results")
races = spark.read.table("silver.dim_race")
circuits = spark.read.table("silver.dim_circuit")

driver_circuit_wins = (
    results
    .filter(
        (F.col("finishing_position") == 1) 
    )
    .join(races,"race_key")
    .filter(F.col("season")>= 2010)
    .join(drivers,"driver_key")
    .join(circuits,"circuitId")
    .groupBy(
        "driverId",
        "circuitId",
        "circuit_name"
    )
    .agg(
        F.count("*").alias("wins_at_circuit"),
        F.avg("fastestLapSpeed").alias("avg_fastest_lap_speed")
    )
)
display(driver_circuit_wins)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT * FROM driver_assets;").show()
spark.sql("UPDATE driver_assets SET flag = 'data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGlkPSJmbGFnLWljb25zLWF1IiB2aWV3Qm94PSIwIDAgNjQwIDQ4MCI+CiAgPHBhdGggZmlsbD0iIzAwMDA4QiIgZD0iTTAgMGg2NDB2NDgwSDB6Ii8+CiAgPHBhdGggZmlsbD0iI2ZmZiIgZD0ibTM3LjUgMCAxMjIgOTAuNUwyODEgMGgzOXYzMWwtMTIwIDg5LjUgMTIwIDg5VjI0MGgtNDBsLTEyMC04OS41TDQwLjUgMjQwSDB2LTMwbDExOS41LTg5TDAgMzJWMHoiLz4KICA8cGF0aCBmaWxsPSJyZWQiIGQ9Ik0yMTIgMTQwLjUgMzIwIDIyMHYyMGwtMTM1LjUtOTkuNXptLTkyIDEwIDMgMTcuNS05NiA3Mkgwek0zMjAgMHYxLjVsLTEyNC41IDk0IDEtMjJMMjk1IDB6TTAgMGwxMTkuNSA4OGgtMzBMMCAyMXoiLz4KICA8cGF0aCBmaWxsPSIjZmZmIiBkPSJNMTIwLjUgMHYyNDBoODBWMHpNMCA4MHY4MGgzMjBWODB6Ii8+CiAgPHBhdGggZmlsbD0icmVkIiBkPSJNMCA5Ni41djQ4aDMyMHYtNDh6TTEzNi41IDB2MjQwaDQ4VjB6Ii8+CiAgPHBhdGggZmlsbD0iI2ZmZiIgZD0ibTUyNyAzOTYuNy0yMC41IDIuNiAyLjIgMjAuNS0xNC44LTE0LjQtMTQuNyAxNC41IDItMjAuNS0yMC41LTIuNCAxNy4zLTExLjItMTAuOS0xNy41IDE5LjYgNi41IDYuOS0xOS41IDcuMSAxOS40IDE5LjUtNi43LTEwLjcgMTcuNnptLTMuNy0xMTcuMiAyLjctMTMtOS44LTkgMTMuMi0xLjUgNS41LTEyLjEgNS41IDEyLjEgMTMuMiAxLjUtOS44IDkgMi43IDEzLTExLjYtNi42em0tMTA0LjEtNjAtMjAuMyAyLjIgMS44IDIwLjMtMTQuNC0xNC41LTE0LjggMTQuMSAyLjQtMjAuMy0yMC4yLTIuNyAxNy4zLTEwLjgtMTAuNS0xNy41IDE5LjMgNi44TDM4NyAxNzhsNi43IDE5LjMgMTkuNC02LjMtMTAuOSAxNy4zIDE3LjEgMTEuMlpNNjIzIDE4Ni43bC0yMC45IDIuNyAyLjMgMjAuOS0xNS4xLTE0LjctMTUgMTQuOCAyLjEtMjEtMjAuOS0yLjQgMTcuNy0xMS41LTExLjEtMTcuOSAyMCA2LjcgNy0xOS44IDcuMiAxOS44IDE5LjktNi45LTExIDE4em0tOTYuMS04My41LTIwLjcgMi4zIDEuOSAyMC44LTE0LjctMTQuOC0xNS4xIDE0LjQgMi40LTIwLjctMjAuNy0yLjggMTcuNy0xMUw0NjcgNzMuNWwxOS43IDYuOSA3LjMtMTkuNSA2LjggMTkuNyAxOS44LTYuNS0xMS4xIDE3LjZ6TTIzNCAzODUuN2wtNDUuOCA1LjQgNC42IDQ1LjktMzIuOC0zMi40LTMzIDMyLjIgNC45LTQ1LjktNDUuOC01LjggMzguOS0yNC44LTI0LTM5LjQgNDMuNiAxNSAxNS44LTQzLjQgMTUuNSA0My41IDQzLjctMTQuNy0yNC4zIDM5LjIgMzguOCAyNS4xWiIvPgo8L3N2Zz4K' WHERE nationality = 'Australian'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT * FROM driver_assets WHERE nationality= 'Australian';").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import base64
import os
image_folder = "raw/Files/f1_source_data/images/flags"
rows = []
for file in os.listdir(image_folder):
   if file.lower().endswith(".png"):
       driver_id = file.replace(".png", "")  # adjust if needed
       with open(os.path.join(image_folder, file), "rb") as f:
           encoded = base64.b64encode(f.read()).decode("utf-8")
       image_string = f"data:image/png;base64,{encoded}"
       rows.append((driver_id, image_string))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import sum,count,when
races = spark.read.table('bronze.races').alias('r')
drivers = spark.read.table('bronze.drivers').alias('d')

top_drivers = (spark.read.table('bronze.results').alias('res')
.join(races, F.col('r.raceId') == F.col("res.raceId"))
.join(drivers,F.col('d.driverId') == F.col("res.driverId"))
.withColumn("driver_name",F.concat(F.col("d.forename"),F.lit(" "),F.col("d.surname")))
.filter(F.col("r.year")>=2010)
.groupBy(F.col("d.driverid"),F.col("driver_name"))
.agg(
    F.count(F.when(F.col("position")=='1',1)).alias("driver_wins")
)
.orderBy(F.col("driver_wins").desc())
.filter(F.col("driver_wins")!=0)
.select(
    F.col("d.driverId"),
    "driver_name",
    "driver_wins"
    
)
)
display(top_drivers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F


def build_constructor_race_status(
    excluded_statuses=None,
    min_season: int = 2010,
    source_schema: str = "silver",
    target_schema: str = "gold",
    target_table: str = "constructor_race_status"
):
    """
    Build the gold constructor race status DataFrame and persist to Lakehouse.

    Parameters
    ----------
    excluded_statuses : list[str], optional
        Status values to exclude. Defaults to +1 to +11 laps.
    min_season : int
        Minimum season year to include.
    source_schema : str
        Schema for source tables.
    target_schema : str
        Schema for target table.
    target_table : str
        Target table name.
    """

    if excluded_statuses is None:
        excluded_statuses = [f"+{i} Laps" for i in range(1, 12)]

    # Load source tables
    results = spark.read.table(f"{source_schema}.fact_race_results").alias("res")
    races   = spark.read.table(f"{source_schema}.dim_race").alias("rac")
    status  = spark.read.table(f"{source_schema}.dim_status").alias("sta")

    # Transform
    df = (
        results
        .join(races, F.col("res.raceId") == F.col("rac.raceId"))
        .join(status, F.col("res.statusId") == F.col("sta.statusId"))
        .filter(F.col("rac.season") >= min_season)
        .filter(~F.col("sta.status").isin(excluded_statuses))
        .select(
            "driverId",
            F.col("res.constructorId").alias("constructor_key"),
            F.col("res.raceId").alias("race_key"),
            F.col("rac.season").alias("season"),
            F.col("res.statusId").alias("status_key"),
            F.when(F.col("sta.status") == "Finished", 1).otherwise(0).alias("finish_flag"),
            F.when(F.col("sta.status") != "Finished", 1).otherwise(0).alias("dnf_flag"),
        )
    )

    # Persist to gold layer
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{target_schema}.{target_table}")
    )

    return df


# Run transformation
gold_df = build_constructor_race_status()

# Display in Fabric notebook
display(gold_df.limit(20))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
