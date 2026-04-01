from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from config import STAGING_PATH, STAGING_SCHEMA


# ==============================================================================
# BRONZE LAYER: RAW DATA INGESTION (NO TRANSFORMATIONS, NO FILTERS)
# ==============================================================================


@dp.table(
    comment="Raw employee data from staging parquet files. Streaming ingestion with Auto Loader. RAW DATA - NO QUALITY FILTERS.",
    # ✅ LIQUID CLUSTERING - mejor que partitioning
    cluster_by=["institucion", "fecha_consulta"],  # Auto-optimiza queries por estas columnas
)
def bronze_contraloria_employees_raw():
    """
    Reads Parquet files from the staging directory.
    This is the entry point for the pipeline.
    
    BRONZE LAYER PRINCIPLES:
    - NO data quality filters (keep bad data for audit)
    - NO transformations (raw as-is from source)
    - Schema enforcement only
    
    Quality checks are applied in SILVER layer.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", f"{STAGING_PATH}/_schemas")
        .schema(STAGING_SCHEMA)
        .load(STAGING_PATH)
         .withColumn(
            "antiguedad",  # Calculate years of service
            F.months_between(F.col("fecha_consulta"), F.col("fecha_de_inicio")) / 12,
        )
    )


# ==============================================================================
# BRONZE LAYER: SCD TYPE 2 - HISTORICAL CHANGE TRACKING
# ==============================================================================

# Create the target streaming table for SCD Type 2
dp.create_streaming_table(
    name="bronze_contraloria_employees_scd_type2",
    comment="Employee records with full change history (SCD Type 2). Tracks changes in salary, allowance, and start date.",
    # ✅ LIQUID CLUSTERING para tabla SCD
    # Optimiza queries por: empleado (keys) + rango de fechas
    cluster_by=["cedula", "institucion", "estado"],
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


# ==============================================================================
# SILVER LAYER: VALIDATED RAW DATA (WITH QUALITY CHECKS)
# ==============================================================================



@dp.materialized_view(
    name="employee_payroll_latest_snapshot",
    comment="Latest employee snapshot with English translations from reference tables. Optimized with broadcast joins.",
    # ✅ LIQUID CLUSTERING para queries analíticas
    cluster_by=["institution_sp", "status_sp"],
)
def employee_payroll_latest_snapshot():
    """
    Creates a snapshot of the most recent employee data with English translations.
    
    OPTIMIZATIONS APPLIED:
    1. ✅ Uses WINDOW FUNCTION instead of collect() for latest timestamp
    2. ✅ Broadcast joins for dimension tables
    3. ✅ Reads from validated silver (not bronze)
    4. ✅ Liquid clustering for analytical queries
    
    This is a MATERIALIZED VIEW (batch processing) that:
    - Filters for the latest query date
    - Joins with reference tables for translations
    - Adds calculated columns (years_of_service)
    """

    # Read validated employee data
    employees_df = spark.read.table("bronze_contraloria_employees_raw")

    # ✅ OPTIMIZACIÓN: Usar window function en lugar de collect()
    # Identifica el último snapshot sin traer datos al driver
    latest_snapshot_window = Window.orderBy(F.col("fecha_consulta").desc())
    
    current_employees_df = (
        employees_df
        .withColumn("row_num", F.row_number().over(latest_snapshot_window))
        .where("row_num = 1")  # ✅ Solo el snapshot más reciente
        .drop("row_num")
    )

    # Load reference tables for translations (small dimension tables)
    institutions_df = spark.read.table(
        "contraloria.reference_and_audit.reference_institution_names"
    )
    statuses_df = spark.read.table(
        "contraloria.reference_and_audit.reference_status_names"
    )
    positions_df = spark.read.table(
        "contraloria.reference_and_audit.reference_position_names"
    )

    # ✅ Broadcast joins for small dimension tables
    return (
        current_employees_df
        .join(
            F.broadcast(institutions_df),
            current_employees_df.institucion == institutions_df.institution_name_spanish,
            "left",
        )
        .join(
            F.broadcast(statuses_df),
            current_employees_df.estado == statuses_df.status_name_spanish,
            "left",
        )
        .join(
            F.broadcast(positions_df),
            current_employees_df.cargo == positions_df.position_name_spanish,
            "left",
        )
        .select(
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


# ==============================================================================
# SILVER LAYER: INACTIVE EMPLOYEES DETECTION (NEW)
# ==============================================================================


@dp.materialized_view(
    name="silver_inactive_employees",
    comment="Employees marked as active in SCD but not present in latest snapshot. Indicates terminations or data quality issues.",
)
def silver_inactive_employees():
    """
    Identifies employees that are:
    - Marked as ACTIVE in SCD Type 2 (__END_AT IS NULL)
    - BUT not present in the latest snapshot
    
    This indicates:
    - Employee termination
    - Data quality issues
    - Missing records in latest API pull
    
    Uses LEFT ANTI JOIN for optimal performance.
    """
    
    # Active employees in SCD (END_AT IS NULL means current version)
    active_in_scd = (
        spark.read.table("bronze_contraloria_employees_scd_type2")
        .where("__END_AT IS NULL")
        .select(
            "cedula",
            "nombre",
            "apellido",
            "cargo",
            "estado",
            "institucion",
            "salario",
            "gasto",
            "fecha_de_inicio",
            "__START_AT",
        )
    )

    # Latest snapshot employees
    latest_snapshot = (
        spark.read.table("employee_payroll_latest_snapshot")
        .select(
            F.col("id_number").alias("cedula"),
            F.col("first_name").alias("nombre"),
            F.col("last_name").alias("apellido"),
            F.col("position_sp").alias("cargo"),
            F.col("status_sp").alias("estado"),
        )
    )

    # ✅ LEFT ANTI JOIN - most efficient way to find non-matching records
    return (
        active_in_scd
        .join(
            latest_snapshot,
            on=["cedula", "nombre", "apellido", "cargo", "estado"],
            how="left_anti",
        )
        .withColumn("detected_at", F.current_timestamp())
        .select(
            "cedula",
            "nombre",
            "apellido",
            "cargo",
            "estado",
            "institucion",
            "salario",
            "gasto",
            "fecha_de_inicio",
            F.col("__START_AT").alias("last_active_date"),
            "detected_at",
        )
    )
