from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    DateType,
    TimestampType,
)
from pyspark.sql.window import Window
from config import STAGING_PATH, STAGING_SCHEMA

# ==============================================================================
# CONFIGURATION
# ==============================================================================

SNAPSHOT_DATE = spark.conf.get("SNAPSHOT_DATE").strip().lower()


# ==============================================================================
# BRONZE LAYER: RAW DATA INGESTION WITH ENGLISH TRANSLATIONS
# ==============================================================================


@dp.table
def bronze_contraloria_employees_raw():
    """
    Reads Parquet files from the staging directory and enriches with English translations.
    Stream-static joins with reference tables for institutions, statuses, and positions.
    All columns are translated to English.
    This is the entry point for the pipeline.
    """

    # Load streaming data from parquet files
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .schema(STAGING_SCHEMA)
        .load(STAGING_PATH)
        .withColumn(
            "antiguedad",
            (F.months_between(F.col("fecha_consulta"), F.col("fecha_de_inicio")) / 12),
        )
    )


# ==============================================================================
# BRONZE LAYER: SCD TYPE 2 - HISTORICAL CHANGE TRACKING
# ==============================================================================

# Create the target streaming table for SCD Type 2
dp.create_streaming_table(
    name="bronze_contraloria_employees_scd_type2",
    comment="Employee records with full change history (SCD Type 2). Tracks changes in salary, allowance, and start date.",
)

# Create Auto CDC flow to track changes
dp.create_auto_cdc_flow(
    target="bronze_contraloria_employees_scd_type2",
    source="bronze_contraloria_employees_raw",
    keys=[
        "cedula",
        "nombre",
        "apellido",
        "cargo",
        "estado",
    ],  # Primary keys for row identification
    sequence_by="fecha_consulta",  # Column for ordering events (timestamp)
    stored_as_scd_type=2,  # Enable SCD Type 2 - adds __START_AT and __END_AT columns
    track_history_column_list=[
        "salario",
        "gasto",
        "fecha_de_inicio",
    ],  # Columns to track history for
)


# Employees with API check information
@dp.table(name="employee_payroll_latest_snapshot")
def employee_payroll_latest_snapshot():

    # Read current employee data (streaming)
    last_snapshot = (
        spark.read.table("contraloria.reference_and_audit.api_check_log_latest")
        .where('run_status = "OK"')
        .select(F.max("source_update").alias("last_source_update"))
    )

    current_employees_df = dp.read("bronze_contraloria_employees_raw").join(
        F.broadcast(last_snapshot),
        on=[F.col("fecha_consulta") == F.col("last_source_update")],
        how="inner",
    )

    # Load reference tables for translations (batch reads for dimension tables)
    institutions_df = spark.read.table(
        "contraloria.reference_and_audit.reference_institution_names"
    ).alias("inst")
    statuses_df = spark.read.table(
        "contraloria.reference_and_audit.reference_status_names"
    ).alias("sts")
    positions_df = spark.read.table(
        "contraloria.reference_and_audit.reference_position_names"
    ).alias("pos")

    # Join with reference tables to add English translations
    return (
        current_employees_df
        # Join with institutions reference table
        .join(
            F.broadcast(institutions_df),
            current_employees_df.institucion
            == institutions_df.institution_name_spanish,
            "left",
        )
        # Join with statuses reference table
        .join(
            F.broadcast(statuses_df),
            current_employees_df.estado == statuses_df.status_name_spanish,
            "left",
        )
        # Join with positions reference table
        .join(
            F.broadcast(positions_df),
            current_employees_df.cargo == positions_df.position_name_spanish,
            "left",
        ).select(
            # Translated columns from Spanish to English
            F.col("nombre").alias("first_name"),
            F.col("apellido").alias("last_name"),
            F.col("cedula").alias("id_number"),
            F.col("salario").alias("salary"),
            F.col("gasto").alias("allowance"),
            # Status columns (Spanish and English)
            F.col("estado").alias("status_sp"),
            F.col("status_name_english").alias("status_en"),
            # Institution columns (Spanish and English)
            F.col("institucion").alias("institution_sp"),
            F.col("institution_name_english").alias("institution_en"),
            # Position columns (Spanish and English)
            F.col("cargo").alias("position_sp"),
            F.col("position_name_english").alias("position_en"),
            F.col("fecha_de_inicio").alias("start_date"),
            F.col("fecha_actualizacion").alias("query_date"),
            F.col("fecha_consulta").alias("snapshot_date"),
            F.col("antiguedad").alias("years_of_service"),
            F.col("archivo").alias("file"),
        )
    )
