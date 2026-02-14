
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, sum, avg, datediff, current_date, concat,lit, upper,regexp_extract
from typing import List


# Pipeline Configuration
PIPELINE_ID = "etl_camera"


CATALOG = "sample"
COMMON_PATH = "/default/data/flox/"
CSVFILE_DIR =  "/Volumes/" + CATALOG + COMMON_PATH + "camera"
TABLE_BRONZE = CATALOG + ".default.bronze_camera"
TABLE_SILVERBASE = CATALOG + ".default.silverbase_camera"
FILENAME_FILTER = "Camera_Farm_"

# ============================================================================
# Ingest VIEWS
# ============================================================================ 
@dp.temporary_view()
def v_camera_csv():
    """Load CSV file from landing into view using Auto Loader"""
    
    df = (
        spark.readStream.format("cloudFiles")
        .option("pathGlobFilter", f"{FILENAME_FILTER}*.csv")
        .option("cloudfiles.format", "CSV")
        .option("header", True)  # Set to True if CSV has header row
        .option("delimiter", ",")
        .option("cloudfiles.maxFilesPerTrigger", 5)
        .option("cloudFiles.inferSchema", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.cleanSource", "DELETE")
        .option("cloudFiles.inferColumnTypes", "true")
        .load(CSVFILE_DIR)
    )

    # Add operational metadata columns
    df = df.withColumn("_processing_timestamp", F.current_timestamp())
    df = df.withColumn("_source_file_name", F.col("_metadata.file_name"))

    return df
# ============================================================================
# TARGET TABLES
# ============================================================================

# Create the streaming table
dp.create_streaming_table(
    name=TABLE_BRONZE,
    comment="Streaming table: camera_bronze",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "quality": "bronze",
    },
)


# Define append flow(s)
@dp.append_flow(
    target=TABLE_BRONZE
    )
def tbl_camera_bronze():
    """Create SCDsnapshot table for camera"""
    # Streaming flow
    df = spark.readStream.table("v_camera_csv")

    return df

# ============================================================================
# STANDARDIZATION VIEWS
# ============================================================================
@dp.temporary_view()
def v_tbl_camera_bronze():
    """Load data from quolive_manager_cases"""
    df = spark.readStream.option("readChangeFeed", "true").table(TABLE_BRONZE)
    return df

@dp.temporary_view()
def v_camera_silver_base():
    """Apply data standardisation to emis_mcameranager_scheme"""
    df = spark.readStream.table("v_tbl_camera_bronze")
    # Apply type casting
    df = df.withColumn("DAY",F.col("DAY").cast("SMALLINT"))
    df = df.withColumn("HOUR",F.col("HOUR").cast("SMALLINT"))
    df = df.withColumn("BRIGHTNESS",F.col("BRIGHTNESS").cast("DECIMAL(6,2)"))
    df = df.withColumn("BIRD_ACTIVITY",F.col("BIRD_ACTIVITY").cast("DECIMAL(6,2)"))
    df = df.withColumn("SOUND_LEVEL",F.col("SOUND_LEVEL").cast("DECIMAL(6,2)"))    
    df = df.withColumn("CAMERA_OPERATIONAL",F.col("CAMERA_OPERATIONAL").cast("BOOLEAN"))
    df = df.withColumn("FARM_NO",regexp_extract("_source_file_name", r"Farm_(\d+)", 1).cast("int"))
    df = df.withColumn("SHED_NO",regexp_extract("_source_file_name", r"Shed_(\d+)", 1).cast("int"))

    return df  
# ============================================================================
# TARGET TABLES SILVER
# ============================================================================

# Create the streaming table for CDC
dp.create_streaming_table(
    name=TABLE_SILVERBASE,
    comment="Streaming table: camera_silver",
)

# CDC mode using auto_cdc
dp.create_auto_cdc_flow(
    target=TABLE_SILVERBASE,
    source="v_camera_silver_base",
    keys=["FARM_NO","SHED_NO","DAY","HOUR"],
    sequence_by="_processing_timestamp",
    stored_as_scd_type=1,
    ignore_null_updates=True,
    except_column_list=["_source_file_name","_rescued_data"],
)
