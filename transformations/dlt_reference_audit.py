from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Target streaming table for latest API check logs
dp.create_streaming_table(name="contraloria.reference_and_audit.api_check_log_latest")

# CDC flow to maintain only the latest record per institution and status
dp.create_auto_cdc_flow(
    target="contraloria.reference_and_audit.api_check_log_latest",
    source="contraloria.reference_and_audit.api_check_log",
    keys=["institution_name_spanish", "status_name_spanish"],
    sequence_by="checked_at",
    stored_as_scd_type=1  # SCD Type 1: mantiene solo el último valor
)

# Reference table with unique institution names, English translations, and last update info
@dp.materialized_view(name="contraloria.reference_and_audit.reference_institution_names")
def reference_institution_names():
    # Get unique institution names with last source_update and checked_at
    institution_stats = (
        spark.read.table("contraloria.reference_and_audit.api_check_log")
         .where('run_status = "OK"')
        .groupBy("institution_name_spanish")
        .agg(
            F.max("source_update").alias("last_source_update"),
            F.max("checked_at").alias("last_checked_at")
        )
    )
    
    # Add English translation using Databricks AI translate function
    return institution_stats.withColumn(
        "institution_name_english",
        F.expr("ai_translate(institution_name_spanish, 'en')")
    )

# Reference table with unique status names, English translations, and last update info
@dp.materialized_view(name="contraloria.reference_and_audit.reference_status_names")
def reference_status_names():
    # Get unique status names with last source_update and checked_at
    status_stats = (
        spark.read.table("contraloria.reference_and_audit.api_check_log")
         .where('run_status = "OK"')
        .groupBy("status_name_spanish")
        .agg(
            F.max("source_update").alias("last_source_update"),
            F.max("checked_at").alias("last_checked_at")
        )
    )
    
    # Add English translation using Databricks AI translate function
    return status_stats.withColumn(
        "status_name_english",
        F.expr("ai_translate(status_name_spanish, 'en')")
    )

# Streaming table to store unique positions (cargos) with append-only pattern
dp.create_streaming_table(name="contraloria.reference_and_audit.reference_positions")

# Temporary view to get distinct positions from the source with translations
@dp.temporary_view()
def unique_positions_stream():
    """
    Extracts distinct position names (cargo) from the bronze employees table
    and adds English translations.
    """
    return (
        spark.readStream.table("contraloria.employee_payroll.bronze_contraloria_raw")
        .select("cargo")
        .where("cargo IS NOT NULL")
        .dropDuplicates(["cargo"])
        .withColumn("position_spanish", F.col("cargo"))
        .withColumn("position_english", F.expr("ai_translate(cargo, 'en')"))
        .withColumn("first_seen", F.current_timestamp())
        .select("position_spanish", "position_english", "first_seen")
    )

# Append flow to insert only new positions into the reference table
@dp.append_flow(
    target="contraloria.reference_and_audit.reference_positions",
    name="new_positions_flow"
)
def append_new_positions():
    """
    Appends only new position values to the reference table.
    The streaming table handles deduplication automatically.
    """
    return spark.readStream.table("unique_positions_stream")
