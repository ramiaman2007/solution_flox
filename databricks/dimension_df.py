
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, sum, avg, datediff, current_date, concat,lit, upper,regexp_extract
import uuid
# Pipeline Configuration
PIPELINE_ID = "etl_dimension"


CATALOG = "sample"
TABLE_SRC_CAMERA = CATALOG + ".default.silverbase_camera"
TABLE_SRC_ENVR = CATALOG + ".default.silverbase_environmental"
TABLE_SRC_SYNTHETIC = CATALOG + ".default.silverbase_synthetic"
TABLE_DIM_FARM = CATALOG + ".default.silver_dimfarm"






# ============================================================================
# TRANSFORM VIEWS
# ============================================================================ 
@dp.temporary_view()
def v_transform_farmnew():



    df_camera = spark.read.option("readChangeFeed", "true").table(TABLE_SRC_CAMERA)
    df_envr = spark.read.option("readChangeFeed", "true").table(TABLE_SRC_ENVR)

    df_camerafarm = df_camera.select('FARM_NO').distinct()
    df_envrfarm = df_envr.select('FARM_NO').distinct()

    df_union= df_camerafarm.union(df_envrfarm)

    try:
        df_farmexst = spark.read.table(TABLE_DIM_FARM)
        df_farmnew = df_union.join(df_farmexst, on="FARM_NO", how="left_anti")
    except Exception:
        # If the table does not exist, treat all as new
        df_farmnew = df_union
        
    df_farmnew = df_farmnew.dropDuplicates(["FARM_NO"])
    df_farmnew = df_farmnew.withColumn('ID',F.expr('uuid()')).withColumn("_processing_timestamp", F.current_timestamp())


    return df_farmnew

# ============================================================================
# TARGET TABLES
# ============================================================================
dp.create_streaming_table(
    name=TABLE_DIM_FARM,
    comment="Snapshot table:Dim Farm",
)

# CDC mode using auto_cdc
dp.create_auto_cdc_from_snapshot_flow(
    target=TABLE_DIM_FARM,
    source="v_transform_farmnew",
    keys=["FARM_NO"],
    stored_as_scd_type=1,
)
