from pyspark.sql import functions as F

GOLD_TABLE = "workspace.default.gold_energy_weather"

df = spark.table(GOLD_TABLE)

# Basic statistics on price and renewable share (min, max, avg)
df.select(
    F.min("price_de_lu"), F.max("price_de_lu"), F.avg("price_de_lu"),
    F.min("renewable_share_pct"), F.max("renewable_share_pct"), F.avg("renewable_share_pct")
).show()

# Extreme days - shows which hours had the highest and lowest renewable share
df.orderBy(F.desc("renewable_share_pct")).select("event_time", "renewable_share_pct", "wind_speed_100m").show(5)
df.orderBy(F.asc("renewable_share_pct")).select("event_time", "renewable_share_pct", "wind_speed_100m").show(5)

# Count hours with negative electricity prices (renewable oversupply)
df.filter(F.col("price_de_lu") < 0).count()

# Correlation of wind speed and temperature with energy price
df.select(
    F.corr("wind_speed_100m", "price_de_lu").alias("wind_price_corr"),
    F.corr("temperature_2m", "price_de_lu").alias("temp_price_corr")
).show()

# Seasonal difference: shows difference in energy production between seasons
df.withColumn("month", F.month("event_time")) \
  .groupBy("month") \
  .agg(F.avg("renewable_share_pct").alias("avg_renewable"), F.avg("price_de_lu").alias("avg_price")) \
  .orderBy("month") \
  .show(12)

# How many hours per year does Germany cover over 80% of consumption
# with renewable sources (wind + solar)?
high_renewable_hours = df.filter(F.col("renewable_share_pct") > 80).count()
total_hours = df.count()
print(f"{high_renewable_hours} hours ({high_renewable_hours / total_hours:.1%}) had >80% renewable share")