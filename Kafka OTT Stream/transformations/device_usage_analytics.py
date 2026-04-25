from pyspark.sql.functions import *
from pyspark.sql.window import Window
import dlt

catalog_name = spark.conf.get("catalog_name")
silver_schemma = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")

@dlt.materialized_view(
    name = f"{catalog_name}.{gold_schema}.device_usage_analytics"
)
def device_usage_analytics():

    df = dlt.read(f"{catalog_name}.{silver_schemma}.watch_events_silverV2")

    watch_time_views = df.where(
        lower(col("event_type")).isin("play", "resume")
    ).groupBy("device").agg(
        round(sum("watch_time_hours"), 2).alias("total_watch_time"),
        count(when(lower(col("event_type")) == "play", True)).alias("total_views")
    )

    window_spec = Window.partitionBy().rowsBetween(
        Window.unboundedPreceding,
        Window.unboundedFollowing
    )

    final_df = watch_time_views.withColumn(
        "complete_watch_time",
        sum("total_watch_time").over(window_spec)
    ).withColumn(
        "watch_time_percentage",
        round(col("total_watch_time") / col("complete_watch_time") * 100, 2)
    ).drop(col("complete_watch_time"))

    return final_df.orderBy("watch_time_percentage", ascending=False)

