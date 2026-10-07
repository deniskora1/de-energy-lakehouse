# Runs on Databricks (PySpark). `spark` is injected by the Databricks
# runtime when this script is executed as a notebook or job — not
# imported here, by design.

from pyspark.sql import functions as F

SILVER_SMARD_TABLE = "workspace.default.smard_silver"
SILVER_OPENMETEO_TABLE = "workspace.default.openmeteo_silver"
GOLD_TABLE = "workspace.default.gold_energy_weather"

df_smard = spark.table(SILVER_SMARD_PATH)
df_openmeteo = spark.table(SILVER_OPENMETEO_PATH)

# Pivot SMARD long-format metrics into feature columns per timestamp
df_smard_pivot = df_smard.groupBy("event_time").pivot("metric").sum("value")

# Spatial average across weather coordinates to match hourly grid intervals
df_weather_avg = df_openmeteo.groupBy("event_time").agg(
    F.round(F.avg("wind_speed_100m"), 2).alias("wind_speed_100m"),
    F.round(F.avg("shortwave_radiation"), 2).alias("shortwave_radiation"),
    F.round(F.avg("temperature_2m"), 2).alias("temperature_2m")
)

df = df_smard_pivot.join(df_weather_avg, "event_time")

# Calculate total renewable share as % of grid load (netzlast)
df = df.withColumn(
    "renewable_share_pct",
    (F.col("wind_onshore") + F.col("wind_offshore") + F.col("photovoltaik")) 
    / F.col("netzlast") 
    * 100
)

# Quality check: fail early if timestamp parsing produces nulls
null_share_count = df.filter(F.col("renewable_share_pct").isNull()).count()
if null_share_count > 0:
    print(f"Warning: {null_share_count} rows have null renewable_share_pct" 
          f"(missing netzlast or renewable values)")

df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(GOLD_TABLE)