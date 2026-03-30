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
    name="silver_contraloria_employees_current",
    comment="Deduplicated current employee records with English column names - keeps most recent per cedula, institucion, nombre, apellido"
)
def silver_contraloria_employees_current():
    """
    Creates a deduplicated view with only the most recent record per employee.
    Deduplication keys: cedula, institucion, nombre, apellido
    Orders by fecha_consulta descending to keep the latest record.
    Translates all column names from Spanish to English.
    Joins with reference tables to get English translations for institutions and statuses.
    """
    # Define window specification to rank records by fecha_consulta within each group
    window_spec = Window.partitionBy(DEDUP_KEYS).orderBy(F.col(UPDATE_COL).desc())
    
    # Get deduplicated employee records
    employees = (
        spark.read.table("bronze_contraloria_employees_scd_type2")
        # Filter for current records only (SCD Type 2)
        .where("__END_AT IS NULL")
        # Add row number to identify the most recent record per group
        .withColumn("row_num", F.row_number().over(window_spec))
        # Keep only the most recent record (row_num = 1)
        .where("row_num = 1")
        # Drop the auxiliary column
        .drop("row_num")
    )
    
    # Read reference tables
    institutions = spark.read.table("contraloria.reference_and_audit.reference_institution_names")
    statuses = spark.read.table("contraloria.reference_and_audit.reference_status_names")
    
    # Join with reference tables to get English translations
    return (
        employees
        .join(institutions, employees.institucion == institutions.institution_name_spanish, "left")
        .join(statuses, employees.estado == statuses.status_name_spanish, "left")
        # Select relevant columns and translate to English
        .select(
            F.col('cedula').alias('id_number'),
            F.coalesce(F.col('institution_name_english'), F.col('institucion')).alias('institution_english'),
            F.col('nombre').alias('first_name'),
            F.col('apellido').alias('last_name'),
            F.col('cargo').alias('position'),
            F.col('salario').alias('salary'),
            F.col('gasto').alias('allowance'),
            F.col('estado').alias('status_spanish'),
            F.coalesce(F.col('status_name_english'), F.col('estado')).alias('status_english'),
            F.col('fecha_de_inicio').alias('start_date'),
            F.col('fecha_actualizacion').alias('update_date'),
            F.col('fecha_consulta').alias('query_date'),
            F.col('archivo').alias('file')
        )
    )