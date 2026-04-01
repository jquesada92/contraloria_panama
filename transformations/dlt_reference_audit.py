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
@dp.table(name="contraloria.reference_and_audit.reference_institution_names")
def reference_institution_names():
    # Get unique institution names with last source_update and checked_at
    institution_stats = (
        spark.readStream.table("contraloria.reference_and_audit.api_check_log")
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
@dp.table(name="contraloria.reference_and_audit.reference_status_names")
def reference_status_names():
    # Get unique status names with last source_update and checked_at
    status_stats = (
        spark.readStream.table("contraloria.reference_and_audit.api_check_log")
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

# Reference table with unique position names and English translations
@dp.table(name="contraloria.reference_and_audit.reference_position_names")
def reference_position_names():
    """
    Extracts unique position names from staging files and translates them once.
    This dramatically improves performance by avoiding ai_translate() on every record.
    """

    
    # Read streaming data from parquet files
    positions = (
        spark.readStream
        .table('contraloria.employee_payroll.bronze_contraloria_employees_raw')
        .select("cargo")
        .filter(F.col("cargo").isNotNull())
        .distinct()
    )
    
    # Add English translation using Databricks AI translate function (once per unique position)
    return positions.withColumn(
        "position_name_english",
        F.expr("ai_translate(cargo, 'en')")
    ).withColumnRenamed("cargo", "position_name_spanish")

