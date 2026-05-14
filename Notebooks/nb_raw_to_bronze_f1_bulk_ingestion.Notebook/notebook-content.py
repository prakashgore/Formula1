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

# # Raw Data Ingestion from Raw Lakehouse to Bronze

# MARKDOWN ********************

# **This Notebook does the task of reading all the uploaded F1 files that are in CSV format and write it directly into the bronze Lakehouse as it is. The First block of code reads CSV files in 'Files/f1_source_data' folder. The next block of code writes the respective CSV files into Bronze Lakehouse as Delta Tables**

# MARKDOWN ********************

# ## Reading CSV files

# CELL ********************

drivers = spark.read.format('csv').option('inferschema','true').option('header','true').load('Files/f1_source_data/drivers.csv')
circuits = spark.read.format('csv').option('inferschema','true').option('header','true').load('Files/f1_source_data/circuits.csv')
constructor_results = spark.read.format('csv').option('inferschema','true').option('header','true').load('Files/f1_source_data/constructor_results.csv')
constructor_standings = spark.read.format('csv').option('inferschema','true').option('header','true').load('Files/f1_source_data/constructor_standings.csv')
constructor = spark.read.format('csv').option('inferschema','true').option('header','true').load('Files/f1_source_data/constructors.csv')
driver_standings = spark.read.format('csv').option('inferschema','true').option('header','true').load('Files/f1_source_data/driver_standings.csv')
lap_times = spark.read.format('csv').option('inferschema','true').option('header','true').load('Files/f1_source_data/lap_times.csv')
pit_stops = spark.read.format('csv').option('inferschema','true').option('header','true').load('Files/f1_source_data/pit_stops.csv')
qualifying = spark.read.format('csv').option('inferschema','true').option('header','true').load('Files/f1_source_data/qualifying.csv')
races = spark.read.format('csv').option('inferschema','true').option('header','true').load('Files/f1_source_data/races.csv')
results = spark.read.format('csv').option('inferschema','true').option('header','true').load('Files/f1_source_data/results.csv')
seasons = spark.read.format('csv').option('inferschema','true').option('header','true').load('Files/f1_source_data/seasons.csv')
sprint_results = spark.read.format('csv').option('inferschema','true').option('header','true').load('Files/f1_source_data/sprint_results.csv')
status = spark.read.format('csv').option('inferschema','true').option('header','true').load('Files/f1_source_data/status.csv')



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Loading CSV files to Delta Tables

# CELL ********************

drivers.write.format('delta').mode('overwrite').saveAsTable('bronze.drivers')
circuits.write.format('delta').mode('overwrite').saveAsTable('bronze.circuits')
constructor_results.write.format('delta').mode('overwrite').saveAsTable('bronze.constructor_results')
constructor_standings.write.format('delta').mode('overwrite').saveAsTable('bronze.constructor_standings')
constructor.write.format('delta').mode('overwrite').saveAsTable('bronze.constructor')
driver_standings.write.format('delta').mode('overwrite').saveAsTable('bronze.driver_standings')
lap_times.write.format('delta').mode('overwrite').saveAsTable('bronze.lap_times')
pit_stops.write.format('delta').mode('overwrite').saveAsTable('bronze.pit_stops')
qualifying.write.format('delta').mode('overwrite').saveAsTable('bronze.qualifying')
races.write.format('delta').mode('overwrite').saveAsTable('bronze.races')
results.write.format('delta').mode('overwrite').saveAsTable('bronze.results')
seasons.write.format('delta').mode('overwrite').saveAsTable('bronze.seasons')
sprint_results.write.format('delta').mode('overwrite').saveAsTable('bronze.sprint_results')
status.write.format('delta').mode('overwrite').saveAsTable('bronze.status')



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
