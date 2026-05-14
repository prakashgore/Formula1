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

BASE_PATH = "abfss://09a0eef0-8957-4dac-9acd-3af916afce13@onelake.dfs.fabric.microsoft.com/81f0def5-3a75-46fc-b336-be49236ef15d/Files/f1_source_data/images/flags"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BinaryType, ArrayType, VarcharType
from pyspark.sql import Row
from datetime import datetime
import base64
import uuid
import json
import pyspark.sql.functions as F
import zlib

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema = StructType([
    StructField("doc_id", StringType(), False),
    StructField("doc_name", StringType(), False),
    StructField("format", StringType(), False),
    StructField("base64_encoding", StringType(), False),
    StructField("processed_at", TimestampType(), False)
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.createDataFrame([], schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

images = mssparkutils.fs.ls(BASE_PATH)

rows = []

for image in images:
    file_df = spark.read.format("binaryFile").load(image.path)
    
    file_bytes = file_df.first()["content"]  
    compressed_bytes = zlib.compress(file_bytes, level=9)
    
    encoded_str = base64.b64encode(compressed_bytes).decode("utf-8")
    
    rows.append(Row(
        doc_id=str(uuid.uuid4()),
        doc_name=image.name.split('.')[0],
        format=image.name.split('.')[1],
        base64_encoding = encoded_str,
        processed_at=datetime.now()
    ))
meta_df = spark.createDataFrame(rows, schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

meta_df = meta_df.select(
    F.col("doc_id")
    ,F.col("doc_name")
    ,F.col("format")
    ,F.col("base64_encoding")
    ,F.col("processed_at")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(meta_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

meta_df.write.format('delta').mode('overwrite').option('mergeSchema','true').option('header','true').saveAsTable('flag_images_64')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.utils import AnalysisException

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def check_table_exists(workspace_id, lakehouse_id, table_name):
    table_path = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Tables/{table_name}"

    try:
        spark.read.format('delta').load(table_path).limit(1).collect()
        return True
    except AnalysisException:
        return False



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_id = "09a0eef0-8957-4dac-9acd-3af916afce13"
lakehouse_id = "81f0def5-3a75-46fc-b336-be49236ef15d"
table_name = "flag_images_64"
result = check_table_exists(workspace_id,lakehouse_id,table_name)
print(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_id = "09a0eef0-8957-4dac-9acd-3af916afce13"
lakehouse_id = "81f0def5-3a75-46fc-b336-be49236ef15d"
table_name = "flag_image_64"
result = check_table_exists(workspace_id,lakehouse_id,table_name)
print(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
