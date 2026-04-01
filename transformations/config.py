# Path to the staging folder where Parquet files are written
# Adjust this to your actual staging path
from pyspark.sql.types import *

ROOT_PATH = '/Workspace/Users/jaquesada92@outlook.com/contraloria_panama'
STAGING_PATH = f'{ROOT_PATH}/staging/'

# ==============================================================================
# SCHEMA DEFINITION - Input Schema (Spanish column names from source files)
# ==============================================================================

STAGING_SCHEMA = StructType([
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
