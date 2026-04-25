from pyspark.sql.functions import *
import dlt

gold_schema = spark.conf.get("gold_schema")
silver_schema = spark.conf.get("silver_schema")
catalog_name = spark.conf.get("catalog_name")

@dlt.table(
    name=f"{catalog_name}.{gold_schema}.content_dropoff_analysis"
)
def content_dropoff_analysis():

    dim_table = dlt.read(f"{catalog_name}.{gold_schema}.content_dim_gold") \
        .select("content_id", "title", "genre", "duration_sec", "release_year") \
        .withColumn("duration_hours", round(col("duration_sec") / 3600, 2)) \
        .drop("duration_sec")

    fact_table = dlt.read(f"{catalog_name}.{silver_schema}.watch_events_silverv2") \
        .where(lower(col("event_type")).isin("play", "resume")) \
        .select("user_id", "content_id", "watch_time_hours")

    agg_fact = fact_table.groupBy("content_id").agg(
        round(sum("watch_time_hours"),2).alias("total_watch_time"),
        countDistinct("user_id").alias("unique_users")
    ).withColumn(
        "avg_watch_time",
        round(
            when(col("unique_users") == 0, 0)
            .otherwise(col("total_watch_time") / col("unique_users")),
            2
        )
    )

    joined = dim_table.join(agg_fact, "content_id", "left") \
        .fillna(0, subset=["total_watch_time", "unique_users", "avg_watch_time"])

    final = joined.withColumn(
        "completion_rate",
        least(
            round(
                when(col("duration_hours") == 0, 0)
                .otherwise(col("avg_watch_time") / col("duration_hours") * 100),
                2
            ),
            lit(100)
        )
    ).withColumn(
        "dropoff_rate",
        round(100 - col("completion_rate"), 2)
    )

    return final.orderBy(col("dropoff_rate").desc())
                         