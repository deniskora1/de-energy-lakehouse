from pyspark.sql import functions as F

RAW_OPENMETEO_PATH = "/Volumes/workspace/default/raw_data/openmeteo/"
SILVER_TABLE = "workspace.default.openmeteo_silver"

df_weather = spark.read.json(RAW_OPENMETEO_PATH)

# Zip parallel arrays within the nested Open-Meteo JSON payload
df_zipped = df_weather.select(
    "location",
    F.arrays_zip(
        "hourly.time", 
        "hourly.wind_speed_100m", 
        "hourly.shortwave_radiation", 
        "hourly.temperature_2m"
    ).alias("hourly_zipped")
)

# Explode zipped arrays into individual rows per time step
df_weather_exploded = df_zipped.select(
    "location",
    F.explode("hourly_zipped").alias("h")
)

# Extract nested fields from struct into flat columns
df_weather_flat = df_weather_exploded.select(
    "location",
    F.col("h.time").alias("event_time_str"),
    F.col("h.wind_speed_100m").alias("wind_speed_100m"),
    F.col("h.shortwave_radiation").alias("shortwave_radiation"),
    F.col("h.temperature_2m").alias("temperature_2m"),
)

# Cast string timestamps to native timestamp type
df_weather_time = df_weather_flat.select(
    "location",
    F.to_timestamp(F.col("event_time_str"), "yyyy-MM-dd'T'HH:mm").alias("event_time"),
    "wind_speed_100m",
    "shortwave_radiation",
    "temperature_2m",
)

# Quality check: fail early if timestamp parsing produces nulls
null_count = df_weather_time.filter(F.col("event_time").isNull()).count()
if null_count > 0:
    raise ValueError(f"Found {null_count} rows with null values in event_time")

df_weather_time.write.format("delta").mode("overwrite").saveAsTable(SILVER_TABLE)