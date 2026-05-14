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

# MARKDOWN ********************

# # **Silver to Gold Business Aggregations Tables**

# MARKDOWN ********************

# **In this notebook we are making Business requirement Aggregations. The tables are ready for Dashboard use. These tables are used in Semantic Model.**

# CELL ********************

from pyspark.sql import functions as F


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Driver Season Profile

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

# MARKDOWN ********************

# ## gold driver top circuit

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Constructor Season

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Constructor Driver DNF

# CELL ********************

dim_status = spark.read.table("silver.dim_status")
excluded_statuses = [
   "+1 Lap", "+2 Laps", "+3 Laps", "+4 Laps", "+5 Laps",
   "+6 Laps", "+7 Laps", "+8 Laps", "+9 Laps", "+10 Laps", "+11 Laps","Collision","Accident"
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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Constructor Race Status

# CELL ********************

from pyspark.sql import functions as F
results = spark.read.table("silver.fact_race_results").alias("res")
races   = spark.read.table("silver.dim_race").alias("rac")
status  = spark.read.table("silver.dim_status").alias("sta")
excluded_statuses = [
   "+1 Lap", "+2 Laps", "+3 Laps", "+4 Laps", "+5 Laps",
   "+6 Laps", "+7 Laps", "+8 Laps", "+9 Laps", "+10 Laps", "+11 Laps"
]
gold_constructor_race_status = (
   results
   .join(races, F.col("res.raceId") == F.col("rac.raceId"))
   .join(status, F.col("res.statusId") == F.col("sta.statusId"))
   .filter(F.col("rac.season") >= 2010)
   # .filter(~F.col("sta.status").isin(excluded_statuses))
   .select(
       "driverId",
       "res.constructorId",
       "res.raceId",
       F.col("rac.season").alias("season"),
       "res.statusId",
       F.when((F.col("sta.status") == "Finished")|(F.col("sta.status").rlike("\\+\\d+ Lap")), 1).otherwise(0).alias("finish_flag"),
       F.when((F.col("sta.status") == "Finished") |(F.col("sta.status").rlike("\\+\\d+ Lap")) , 0).otherwise(1).alias("dnf_flag"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dim Season

# CELL ********************


dim_season = (spark.read.table('silver.dim_race')
.select(F.col("season"))
.distinct()
.filter(F.col("season")>=2010)
.withColumn("season_key",F.col("season"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold dim circuit

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold fact circuit race profile

# CELL ********************


from pyspark.sql import functions as F


res = spark.read.table('silver.fact_race_results').alias('res')
r   = spark.read.table('silver.dim_race').alias('r')
st  = spark.read.table('silver.dim_status').alias('st')


exclude_statuses = ['+1 Lap', '+2 Laps', '+3 Laps', '+4 Laps', '+5 Laps',
                    '+6 Laps', '+7 Laps', '+8 Laps', '+11 Laps']

gold_fact_circuit_race_profile = (
    res
    .join(r, F.col('res.raceId') == F.col('r.raceId'))     
    .join(st, F.col('res.statusId') == F.col('st.statusId'))
    .filter(F.col('r.season') >= 2010)
    .filter(~F.col('st.status').isin(exclude_statuses))     
    .groupBy(F.col('r.circuitId'), F.col('r.season'))       
    .agg(
        F.count(F.lit(1)).alias('race_entries'),
        F.sum(F.when(F.col('res.finishing_position') == 1, 1).otherwise(0)).alias('wins'),
        F.avg(F.when(F.col('res.finishing_position') == 1, F.col('res.grid_position'))).alias('avg_grid_pos_winner'),
        F.avg(F.col('res.grid_position') - F.col('res.finishing_position')).alias('avg_positions_gained'),
        F.sum(F.when(F.col('st.status') != 'Finished', 1).otherwise(0)).alias('total_dnfs'),
        F.sum(F.when(F.col('st.status') == 'Accident', 1).otherwise(0)).alias('accident_dnfs'),
        F.sum(F.when(F.col('st.status').isin('Engine','Gearbox','Hydraulics','Transmission','Electrical','Brakes'), 1).otherwise(0)).alias('mechanical_dnfs')
    )
    .select(
        F.col('r.circuitId').alias('circuitId'),
        F.col('r.season').alias('season'),
        F.col('race_entries'),
        F.col('wins'),
        F.col('avg_grid_pos_winner').cast('int').alias('avg_grid_pos_winner'),
        F.col('avg_positions_gained').cast('int').alias('avg_positions_gained'),
        F.col('total_dnfs'),
        F.col('accident_dnfs'),
        F.col('mechanical_dnfs')
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold circuit overtaking

# CELL ********************


from pyspark.sql import functions as F
results = spark.read.table('silver.fact_race_results').alias('res')
races   = spark.read.table('silver.dim_race').alias('r')

gold_circuit_overtaking = (
    results
    .join(races, F.col('res.raceId') == F.col('r.raceId')) 
    .filter(F.col('r.season') >= 2010)
    .groupBy(F.col('r.circuitId'), F.col('r.season'))
    .agg(
        F.avg(F.col('res.grid_position') - F.col('res.finishing_position')).alias('avg_positions_gained'),
        F.sum(F.when((F.col('res.grid_position') - F.col('res.finishing_position')) >= 5, 1).otherwise(0)).alias('big_overtakes')
    )
    .select(
        F.col('r.circuitId').alias('circuitId'),
        F.col('r.season').alias('season'),
        F.col('avg_positions_gained').cast('int').alias('avg_positions_gained'),
        F.col('big_overtakes')
    )
)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Gold fact circuit strategy index

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

driver_season_profile.write.format('delta').mode('overwrite').option("overwriteSchema","true").saveAsTable("driver_season_profile")
driver_top_circuit.write.format('delta').mode('overwrite').option('header','true').saveAsTable('gold_driver_top_circuits')
gold_constructor_race_status.write.format('delta').mode('overwrite').option("overwriteSchema","true").saveAsTable("gold_constructor_race_status")
constructor_driver_dnf.write.format('delta').mode('overwrite').option("header","true").saveAsTable("gold_constructor_driver_dnf")
constructor_season.write.format('delta').mode('overwrite').saveAsTable("constructor_season")
dim_season.write.mode('overwrite').format('delta').saveAsTable("gold_dim_season")
gold_dim_circuit.write.format('delta').mode('overwrite').saveAsTable('gold_dim_circuit')
gold_fact_circuit_race_profile.write.format('delta').mode('overwrite').saveAsTable('gold_fact_circuit_race_profile')
gold_circuit_overtaking.write.format('delta').mode('overwrite').saveAsTable('gold_circuit_overtaking')
gold_fact_circuit_strategy_index.write.format('delta').mode('overwrite').saveAsTable('gold_fact_circuit_strategy_index')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
