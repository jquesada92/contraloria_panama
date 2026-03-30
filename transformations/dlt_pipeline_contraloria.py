from pyspark import pipelines as dp
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, TimestampType
from pyspark.sql.window import Window

# ==============================================================================
# CONFIGURATION
# ==============================================================================

# Path to the staging folder where Parquet files are written
# Adjust this to your actual staging path
ROOT_PATH = '/Workspace/Users/jaquesada92@outlook.com/contraloria_panama'
STAGING_PATH = f'{ROOT_PATH}/staging/'


# Primary keys used for SCD Type 2 tracking (Spanish names)
KEY_COLS = ['cedula', 'institucion']

# Deduplication keys for silver table
DEDUP_KEYS = ['cedula', 'institucion', 'nombre', 'apellido']

# Column used to track updates/changes (Spanish name)
UPDATE_COL = 'fecha_consulta'

# ==============================================================================
# SCHEMA DEFINITION - Input Schema (Spanish column names from source files)
# ==============================================================================

schema = StructType([
    StructField('nombre', StringType(), True),
    StructField('apellido', StringType(), True),
    StructField('cedula', StringType(), True),
    StructField('cargo', StringType(), True),
    StructField('salario', DoubleType(), True),
    StructField('gasto', DoubleType(), True),
    StructField('estado', StringType(), True),
    StructField('fecha_de_inicio', DateType(), True),
    StructField('fecha_actualizacion', TimestampType(), True),
    StructField('fecha_consulta', TimestampType(), True),
    StructField('archivo', StringType(), True),
    StructField('institucion', StringType(), True)
])

# ==============================================================================
# BRONZE LAYER: RAW DATA INGESTION (SPANISH COLUMN NAMES)
# ==============================================================================


dp.create_streaming_table("bronze_contraloria_raw",
                          comment="New customer data incrementally ingested from cloud object storage landing zone")

# Create an Append Flow to ingest the raw data into the bronze table
@dp.append_flow(
  target = "bronze_contraloria_raw",
  name = "customers_bronze_ingest_flow"
)
def contraloria_employees_raw_bronze():
    """
    Reads Parquet files from the staging directory.
    Keeps original Spanish column names as they appear in source files.
    This is the entry point for the pipeline.
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .schema(schema)
        .load(STAGING_PATH)
    )

# ==============================================================================
# BRONZE LAYER: SCD TYPE 2 TABLE (SPANISH COLUMN NAMES)
# ==============================================================================
dp.create_streaming_table(name="bronze_contraloria_employees_scd_type2")

dp.create_auto_cdc_flow(
    target="bronze_contraloria_employees_scd_type2",
    source="bronze_contraloria_raw",
    keys=KEY_COLS,  # Primary keys: cedula, institucion
    sequence_by=UPDATE_COL,  # Sequence by: fecha_consulta
    stored_as_scd_type="2",  # Enable SCD Type 2
    track_history_column_list=[  # Columns to track for changes (Spanish names)
        'nombre', 'apellido', 'cargo', 'salario', 'gasto', 
        'estado', 'fecha_de_inicio', 'fecha_actualizacion'
    ]
)

# ==============================================================================
# SILVER LAYER: DEDUPLICATED CURRENT EMPLOYEES (ENGLISH COLUMN NAMES)
# ==============================================================================

@dp.materialized_view(
    name="silver_contraloria_employees_deduped",
    comment="Deduplicated current employee records with English translations - keeps most recent per employee ID, institution, first name, and last name"
)
def silver_contraloria_employees_deduped():
    """
    Creates a deduplicated view with only the most recent record per employee.
    
    Deduplication keys: cedula, institucion, nombre, apellido
    Sorting: fecha_consulta descending to keep the latest record.
    
    Enriches employee data with English translations for:
    - Institution names (via reference_institution_names)
    - Employment statuses (via reference_status_names)
    - Position titles (via reference_positions)
    
    Returns:
        DataFrame with deduplicated employee records and English translations
    """
    # Define window specification to rank records by query date within each employee group
    window_spec = Window.partitionBy(DEDUP_KEYS).orderBy(F.col(UPDATE_COL).desc())
    
    # Get deduplicated employee records from SCD Type 2 bronze table
    employees = (
        spark.read.table("bronze_contraloria_employees_scd_type2")
        # Filter for current records only (exclude historical versions)
        .where("__END_AT IS NULL")
        # Add row number to identify the most recent record per employee
        .withColumn("row_num", F.row_number().over(window_spec))
        # Keep only the most recent record (row_num = 1)
        .where("row_num = 1")
        # Drop the auxiliary ranking column
        .drop("row_num")
    )
    
    # Load reference tables for translations
    institutions = spark.read.table("contraloria.reference_and_audit.reference_institution_names").alias('inst')
    statuses = spark.read.table("contraloria.reference_and_audit.reference_status_names").alias('sts')
    positions = spark.read.table("contraloria.reference_and_audit.reference_positions").alias('pos')
    
    # Join with reference tables to enrich with English translations
    return (
        employees
        .join(F.broadcast(institutions), employees.institucion == institutions.institution_name_spanish, "left")
        .join(F.broadcast(statuses), employees.estado == statuses.status_name_spanish, "left")
        .join(F.broadcast(positions), employees.cargo == positions.position_spanish, "left")
        # Calculate the most recent snapshot date from reference tables
        .withColumn('last_snapshot', F.greatest(F.col('inst.last_source_update'), F.col('sts.last_source_update')))
        # Select and rename columns with English aliases
        .select(
            F.col('cedula').alias('id_number'),
            F.coalesce(F.col('institution_name_english'), F.col('institucion')).alias('institution_english'),
            F.col('nombre').alias('first_name'),
            F.col('apellido').alias('last_name'),
            F.coalesce(F.col('position_english'), F.col('cargo')).alias('position_english'),
            F.col('salario').alias('salary'),
            F.col('gasto').alias('allowance'),
            F.coalesce(F.col('status_name_english'), F.col('estado')).alias('status'),
            F.col('fecha_de_inicio').alias('start_date'),
            F.col('fecha_actualizacion').alias('update_date'),
            F.col('fecha_consulta').alias('query_date'),
            # Mark employee as active if their query date matches or exceeds the last snapshot
            F.when(F.col('fecha_consulta') >= F.col('last_snapshot'), True).otherwise(False).alias('is_active')
        )
    )

