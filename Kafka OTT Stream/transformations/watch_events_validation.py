from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

silver_schema = spark.conf.get("silver_schema")
bronze_schema = spark.conf.get("bronze_schema")
catalog_name = spark.conf.get("catalog_name")

@dlt.table(
    name=f"{catalog_name}.{silver_schema}.watch_events_quarantineV2"
)
def watch_events_quarantine():

    df = dlt.read_stream(f"{catalog_name}.{bronze_schema}.watch_events_bronzeV2")

    return df.withColumn(
        "error_reason",
        when(col("content_id").isNull(), "Missing content_id")
        .when(col("user_id").isNull(), "Missing user_id")
        .when(col("title").isNull(), "Missing title")
        .when(col("watch_time_sec").isNull(), "Missing watch_time_sec")
        .when(col("event_time").isNull(), "Missing event_time")
        .when(col("event_type").isNull(), "Missing event_type")
        .when(col("watch_time_sec") <= 0, "Negative watch_time")
    ).filter(col("error_reason").isNotNull()) \
     .withColumn("quarantine_time", current_timestamp())


@dlt.table(
    name=f"{catalog_name}.{silver_schema}.watch_events_silverV2"
)
@dlt.expect_or_drop(
    "valid_not_null",
    "content_id IS NOT NULL AND user_id IS NOT NULL AND title IS NOT NULL AND watch_time_sec IS NOT NULL AND event_time IS NOT NULL AND event_type IS NOT NULL"
)
@dlt.expect_or_drop(
    "valid_watch_time",
    "watch_time_sec > 0 AND watch_time_sec <= 14400"
)
def watch_events_silver():

    df = dlt.read_stream(f"{catalog_name}.{bronze_schema}.watch_events_bronzeV2")

    for c in ["title", "event_type", "device"]:
        df = df.withColumn(c, trim(lower(col(c))))

    for c in ["content_id", "user_id"]:
        df = df.withColumn(c, trim(col(c)))

    df = df.withColumn(
        "device",
        when(col("device").rlike("mobile|android|iphone"), "Mobile")
        .when(col("device").rlike("tv|smarttv"), "TV")
        .when(col("device").rlike("windows|mac|linux"), "PC")
        .when(col("device").rlike("ipad|tablet"), "Tablet")
        .when(col("device").rlike("web"), "Internet")
        .otherwise("Unknown")
    ).fillna("Unknown", ["device"])

    valid_events = ["play", "pause", "stop"]

    df = df.withColumn(
        "event_type",
        when(col("event_type").isin(valid_events), initcap(col("event_type")))
        .otherwise("Unknown")
    )

    df = df.withWatermark("event_time", "10 minutes") \
        .dropDuplicates(["user_id", "content_id", "event_time"])

    df = df.withColumn("watch_time_minutes", round(col("watch_time_sec") / 60, 2)) \
           .withColumn("watch_time_hours", round(col("watch_time_sec") / 3600, 2)) \
           .withColumn("event_date", to_date(col("event_time"))) \
           .withColumn("event_hour", hour(col("event_time")))

    return df.withColumn("validated_at", current_timestamp())