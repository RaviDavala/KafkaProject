from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")
catalog_name = spark.conf.get("catalog_name")

@dlt.table(name=f"{catalog_name}.{silver_schema}.content_dim_quarantine")
def content_dim_quarantine():

    df = dlt.read_stream("kafka_ott.bronze.content_dim_bronze")

    return df.filter(
        col("content_id").isNull() |
        col("genre").isNull() |
        col("title").isNull() |
        col("duration_sec").isNull() |
        col("release_year").isNull()
    ).withColumn("error_reason", lit("Null values in mandatory fields")) \
     .withColumn("quarantine_time", current_timestamp())



@dlt.table(name=f"{catalog_name}.{silver_schema}.content_dim_silver")
@dlt.expect_or_drop(
    "Null data",
    "content_id is not null AND genre is not null AND title is not null AND duration_sec is not null AND release_year is not null"
)
@dlt.expect_or_drop(
    "Valid duration",
    "duration_sec > 0")
    
def content_dim_silver():

    df_bronze = dlt.read_stream("kafka_ott.bronze.content_dim_bronze")
    
    # For ID (safe normalization)
    df_bronze = df_bronze.withColumn(
        "content_id",
        trim(col("content_id"))
    )

    # For text fields (presentation cleanup)
    for c in ["title", "genre"]:
        df_bronze = df_bronze.withColumn(
            c,
            initcap(lower(trim(col(c))))
        )

    return df_bronze.withColumn("validated_at", current_timestamp())



dlt.create_streaming_table(
    name=f"{catalog_name}.{gold_schema}.content_dim_gold"
)

dlt.apply_changes(
    target=f"{catalog_name}.{gold_schema}.content_dim_gold",
    source=f"{catalog_name}.{silver_schema}.content_dim_silver",
    keys=["content_id"],
    sequence_by=col("ingested_at"),
    stored_as_scd_type=1
)

