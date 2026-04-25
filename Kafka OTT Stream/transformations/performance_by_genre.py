from pyspark.sql.functions import *
import dlt

gold_schema = spark.conf.get("gold_schema")
silver_schema = spark.conf.get("silver_schema")
catalog_name = spark.conf.get("catalog_name")

@dlt.table(
    name=f"{catalog_name}.{gold_schema}.performance_by_genre"
)
def performance_by_genre():

    facts = dlt.read(f"{catalog_name}.{silver_schema}.watch_events_silverv2")
    dim = dlt.read(f"{catalog_name}.{gold_schema}.content_dim_gold")

    joined = (
        facts.join(dim, on="content_id")
        .withColumn("event_type_clean", lower(col("event_type")))
        .where(col("event_type_clean").isin("play", "resume"))
    )

    agg_df = joined.groupBy("genre").agg(
        round(sum("watch_time_hours"), 2).alias("total_watch_time"),
        count(when(col("event_type_clean") == "play", True)).alias("total_views"),
        countDistinct("user_id").alias("unique_users")
    )

    final = agg_df.withColumn(
        "avg_watch_time_per_user",
        round(
            when(col("unique_users") == 0, 0)
            .otherwise(col("total_watch_time") / col("unique_users")),
            2
        )
    )

    return final.orderBy(col("total_watch_time").desc(), col("total_views").desc())
    