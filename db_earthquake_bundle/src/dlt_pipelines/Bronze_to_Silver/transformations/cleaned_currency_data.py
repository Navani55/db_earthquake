import dlt
from pyspark.sql.functions import col, explode, from_json, element_at
from pyspark.sql.types import StructType, StructField, StringType, MapType, ArrayType

volume_path = "/Volumes/earthquake/bronze/db_countries_currency"
# Define schemas outside the function — cleaner and reusable
name_schema = StructType([
    StructField("common", StringType(), True),
    StructField("official", StringType(), True),
])
currency_schema = MapType(
    StringType(),
    StructType([
        StructField("name", StringType(), True),
        StructField("symbol", StringType(), True),
    ]),
)
capital_schema = ArrayType(StringType())

@dlt.table(
    name="country_currency",
    comment="Flattened country-currency mapping from REST Countries API",
)
def country_currency():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{volume_path}/")
    )
    core_cols = (
        df.withColumn("name_parsed", from_json(col("name"), name_schema))
        .withColumn("currencies_parsed", from_json(col("currencies"), currency_schema))
        .withColumn("capital_parsed", from_json(col("capital"), capital_schema))
        # Guard against null currencies before explode — prevents pipeline failure
        .filter(col("currencies_parsed").isNotNull())
        .select(
            col("name_parsed.common").alias("country"),
            col("name_parsed.official").alias("country_official"),
            element_at("capital_parsed", 1).alias("capital"),
            explode("currencies_parsed").alias("currency_code", "currency_details"),
        )
        .select(
            "country",
            "country_official",
            "capital",
            "currency_code",
            col("currency_details.name").alias("currency_name"),
            col("currency_details.symbol").alias("currency_symbol"),
        )
    )
    # Project out ONLY the CDC columns to ensure strict schema match
    return core_cols.select(
        "country",
        "country_official",
        "capital",
        "currency_code",
        "currency_name",
        "currency_symbol",
    )

# Create the target streaming table for CDC

# Apply Auto CDC flow with SCD Type 1 (direct updates, no history)
dlt.create_streaming_table(
    name="country_currency_cdc",
    schema="country STRING, country_official STRING, capital STRING, currency_code STRING, currency_name STRING, currency_symbol STRING"
)
dlt.create_auto_cdc_flow(
    target="country_currency_cdc",
    source="country_currency",
    keys=["country", "currency_code"],
    sequence_by=col("capital"),  # Replace with a proper sequencing column if available
    stored_as_scd_type=1
)
