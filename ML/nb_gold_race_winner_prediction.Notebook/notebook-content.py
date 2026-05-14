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
from pyspark.ml.functions import vector_to_array
df = spark.read.table("gold_ml_features")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

LABEL_COL = "IS_WINNER"
FEATURE_LABLES = [
                "GRID_POSITION",
                "TOTAL_DRIVER_RACES",
                "TOTAL_DRIVER_WINS",
                "TOTAL_DRIVER_POINTS",
                "AVG_DRIVER_GRID_POSITION",
                "AVG_DRIVER_FINISH_POSITION",
                "CONSTRUCTOR_DNF_RATE",
                "CIRCUIT_DNF_RATE",
                "RECENT_AVG_POINTS",
                "RECENT_AVG_FINISH_POSITION"
                ]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.ml.feature import VectorAssembler
assembler = VectorAssembler(
    inputCols=FEATURE_LABLES,
    outputCol="features"
)
df_ml = assembler.transform(df).select(
    "RACEID",
    "SEASON",
    "DRIVERID",
    "CONSTRUCTORID",
    "features",
    LABEL_COL
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

train_df, test_df = df_ml.randomSplit([0.8,0.2],seed=42)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(
    featuresCol="features",
    labelCol=LABEL_COL,
    probabilityCol="win_probability",
    predictionCol="predicted_win"
)
model = lr.fit(train_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

predictions = model.transform(test_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_predictions = (
   predictions
   .withColumn("win_probability", vector_to_array("win_probability")[1])
   .select(
       "RACEID",
       "SEASON",
       "DRIVERID",
       "CONSTRUCTORID",
       F.col("predicted_win").alias("PREDICTED_WIN"),
       F.col("win_probability").alias("WIN_PROBABILITY"),
       F.col(LABEL_COL).alias("ACTUAL_IS_WINNER")
   )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(final_predictions)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
