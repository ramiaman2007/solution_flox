
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, sum, avg, datediff, current_date, concat,lit, upper,regexp_extract
from typing import List


# Pipeline Configuration
PIPELINE_ID = "etl_synthetic"



CATALOG = "sample"
COMMON_PATH = "/default/data/flox/"
CSVFILE_DIR =  "/Volumes/" + CATALOG + COMMON_PATH + "synthetic"
TABLE_BRONZE = CATALOG + ".default.bronze_synthetic"
TABLE_SILVERBASE = CATALOG + ".default.silverbase_synthetic"
FILENAME_FILTER = "Synthetic_Poultry_"

# ============================================================================
# Ingest VIEWS
# ============================================================================ 
@dp.temporary_view()
def v_synthetic_csv():
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

    # Clean column names: replace forbidden characters
    for c in df.columns:
        clean = c.replace("(", "_").replace(")", "").replace(" ", "_")
        if clean != c:
            df = df.withColumnRenamed(c, clean)

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
    comment="Streaming table: synthetic_bronze",
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
def tbl_synthetic_bronze():
    """Create SCDsnapshot table for camera"""
    # Streaming flow
    df = spark.readStream.table("v_synthetic_csv")

    return df


# ============================================================================
# STANDARDIZATION VIEWS
# ============================================================================
@dp.temporary_view()
def v_tbl_synthetic_bronze():
    """Load data from synthetic bronze"""
    df = spark.readStream.option("readChangeFeed", "true").table(TABLE_BRONZE)
    return df

@dp.temporary_view()
def v_synthetic_silver_base():
    """Apply data standardisation to emis_manager_scheme"""
    df = spark.readStream.table("v_tbl_synthetic_bronze")
    # Apply type casting
    df = df.withColumn("DAY",F.col("Day").cast("SMALLINT"))
    df = df.withColumn("WEIGHT_GM",F.col("Weight_g").cast("DECIMAL(6,2)"))
    df = df.withColumn("FARM_NO",regexp_extract("_source_file_name", r"Farm_(\d+)", 1).cast("int"))
    df = df.withColumn("SHED_NO",regexp_extract("_source_file_name", r"Shed_(\d+)", 1).cast("int"))
    return df  
# ============================================================================
# TARGET TABLES SILVER
# ============================================================================

# Create the streaming table for CDC
dp.create_streaming_table(
    name=TABLE_SILVERBASE,
    comment="Streaming table: synthetic_silver",
)

# CDC mode using auto_cdc
dp.create_auto_cdc_flow(
    target=TABLE_SILVERBASE,
    source="v_synthetic_silver_base",
    keys=["FARM_NO","SHED_NO","DAY"],
    sequence_by="_processing_timestamp",
    stored_as_scd_type=1,
    ignore_null_updates=True,
    except_column_list=["_source_file_name","_rescued_data"],
)
