from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

#importing all the required config's from the pipeline parameters
bootstrap = spark.conf.get("bootstrap_server")
api_key = spark.conf.get("kafka_api_key")
api_secret = spark.conf.get("kafka_secret")
events_topic = spark.conf.get("kafka_topic_events")
dim_topic = spark.conf.get("kafka_topic_dim")
catalog_name = spark.conf.get("catalog_name")
bronze_schema = spark.conf.get("bronze_schema")

#creates a new bronze table if it doesn't exist in the given schema
@dlt.table(
    name=f"{catalog_name}.{bronze_schema}.watch_events_bronzeV2"
)
def kafka_bronze_events():

    df_events = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap) \
        .option("subscribe", events_topic) \
        .option("startingOffsets", "earliest") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option(
            "kafka.sasl.jaas.config",
            f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
            f"required username='{api_key}' password='{api_secret}';"
        ) \
        .load()

    df = df_events.selectExpr("CAST(value AS STRING)")

    schema = StructType([
        StructField("user_id", StringType()),
        StructField("content_id", StringType()),
        StructField("title", StringType()),
        StructField("watch_time_sec", DoubleType()),
        StructField("event_type", StringType()),
        StructField("device", StringType()),
        StructField("event_time", DoubleType())
    ])

    df = df.withColumn("json", from_json(col("value"), schema)).select("json.*")

    df = df.withColumn(
        "event_time",
        expr("timestamp_seconds(event_time)")
    )

    return df.withColumn("ingested_at", current_timestamp())

#second bronze table for loading the dimension data from the second kafka topic
@dlt.table(
    name = f"{catalog_name}.{bronze_schema}.content_dim_bronze"
)
def kafka_bronze_content_dim():
    df_raw = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap) \
        .option("subscribe", dim_topic) \
        .option("startingOffsets", "earliest") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option(
            "kafka.sasl.jaas.config",
            f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule "
            f"required username='{api_key}' password='{api_secret}';"
        ) \
        .load()

    df = df_raw.select(col("value").cast("String"))

    schema = StructType([
        StructField("content_id", StringType()),
        StructField("title", StringType()),
        StructField("genre", StringType()),
        StructField("duration_sec", DoubleType()),
        StructField("release_year", StringType())
    ])

    df = df.withColumn("json", from_json(col("value"), schema))

    df = df.select("json.*")

    return df.withColumn("ingested_at", current_timestamp())
