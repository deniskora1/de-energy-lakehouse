# Runs on Databricks (PySpark). `spark` is injected by the Databricks
# runtime when this script is executed as a notebook or job — not
# imported here, by design.

from pyspark.sql import functions as F

RAW_SMARD_PATH = "/Volumes/workspace/default/raw_data/smard/"
SILVER_TABLE = "workspace.default.smard_silver"

df = spark.read.json(RAW_SMARD_PATH)

# Unnest time series arrays from JSON payload into individual rows
df_exploded = df.select("filter", F.explode("series").alias("pair"))

# Extract epoch timestamp in milliseconds and corresponding metric value
df_split = df_exploded.select(
    "filter",
    F.col("pair")[0].alias("timestamp_ms"),
    F.col("pair")[1].alias("value"),
)

# Convert Unix timestamp from milliseconds to standard timestamp format
df_time = df_split.select(
    "filter",
    F.from_unixtime((F.col("timestamp_ms") / 1000).cast("long")).alias("event_time"),
    "value",
)

# Map numerical SMARD filter codes to human-readable metric names
df_named = df_time.select(
    F.col("filter").alias("metric_code"),
    F.when(F.col("filter") == 4067, "wind_onshore") \
     .when(F.col("filter") == 1225, "wind_offshore") \
     .when(F.col("filter") == 4068, "photovoltaik") \
     .when(F.col("filter") == 410, "netzlast") \
     .when(F.col("filter") == 4169, "price_de_lu")
    .otherwise("unknown").alias("metric"),
    "event_time",
    "value",
)

# Quality check: ensure all records are correctly mapped to known metrics
unknown_count = df_named.filter(F.col("metric") == "unknown").count()
if unknown_count > 0:
    raise ValueError(f"Found {unknown_count} rows with unknown metric_code")

df_named.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)