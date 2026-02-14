
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, sum, avg, datediff, current_date, concat,lit, upper,regexp_extract
from typing import List


# Pipeline Configuration
PIPELINE_ID = "etl_analytics"


CATALOG = "sample"
TABLE_SRC_CAMERA = CATALOG + ".default.silverbase_camera"
TABLE_SRC_ENVR = CATALOG + ".default.silverbase_environmental"
TABLE_SRC_SYNTHETIC = CATALOG + ".default.silverbase_synthetic"
TABLE_AGGR_ENVRACTIVITY = CATALOG + ".default.gold_agg_envirmentalactivity"
TABLE_ANALYTICS_ENVRACTIVITY = CATALOG + ".default.gold_envirmentalactivity"


# ============================================================================
# Source VIEWS
# ============================================================================ 
@dp.temporary_view()
def v_src_camera_silverbase():
    """Load data from quolive_manager_cases"""
    df = spark.read.option("readChangeFeed", "true").table(TABLE_SRC_CAMERA)
    df=df.drop("_processing_timestamp","_change_type","_commit_version","_commit_timestamp")
    return df

@dp.temporary_view()
def v_src_environmental_silverbase():
    """Load data from quolive_manager_cases"""
    df = spark.read.option("readChangeFeed", "true").table(TABLE_SRC_ENVR)
    df=df.drop("_processing_timestamp","_change_type","_commit_version","_commit_timestamp")    
    return df

@dp.temporary_view()
def v_src_synthetic_silverbase():
    """Load data from quolive_manager_cases"""
    df = spark.read.option("readChangeFeed", "true").table(TABLE_SRC_SYNTHETIC)
    df=df.drop("_processing_timestamp","_change_type","_commit_version","_commit_timestamp")    
    return df    


# ============================================================================
# TRANSFORM VIEWS
# ============================================================================ 
@dp.temporary_view()
def v_join_environmntalactivity():

    df_camera = spark.read.table("v_src_camera_silverbase")
    df_envr = spark.read.table("v_src_environmental_silverbase")

    df_join = (
    df_envr.alias('e')
    .join(
        df_camera.alias('c'),
        (
            (df_envr.FARM_NO == df_camera.FARM_NO) &
            (df_envr.SHED_NO == df_camera.SHED_NO)  &
            (df_envr.DAY == df_camera.DAY) &
            (df_envr.HOUR == df_camera.HOUR)
        ),
        'inner'
    )
    .select('e.DAY','e.HOUR','e.FARM_NO','e.SHED_NO','c.BIRD_ACTIVITY','c.SOUND_LEVEL','e.TEMPRATURE', 'e.HUMIDITY')
    )
    return df_join


@dp.temporary_view()
def v_aggregate_environmntalactivity():
    df = spark.read.table("v_join_environmntalactivity")
    group_cols =["FARM_NO","SHED_NO","DAY"]
    df_agg = df.groupBy(group_cols).agg(
                                avg("BIRD_ACTIVITY").alias("AVG_BIRD_ACTIVITY"),
                                avg("SOUND_LEVEL").alias("AVG_SOUND_LEVEL"),
                                avg("TEMPRATURE").alias("AVG_TEMPRATURE"),
                                avg("HUMIDITY").alias("AVG_HUMIDITY")
                                   
    )
    return df_agg

# ============================================================================
# TARGET TABLES
# ============================================================================
dp.create_streaming_table(
    name=TABLE_AGGR_ENVRACTIVITY,
    comment="Streaming table: Aggregate EnvironmentalActivitye",
)

# CDC mode using auto_cdc
dp.create_auto_cdc_from_snapshot_flow(
    target=TABLE_AGGR_ENVRACTIVITY,
    source="v_aggregate_environmntalactivity",
    keys=["FARM_NO","SHED_NO","DAY"],
    stored_as_scd_type=1,
)

# Create the streaming table for CDC
dp.create_streaming_table(
    name=TABLE_ANALYTICS_ENVRACTIVITY,
    comment="Streaming table: EnvironmentalActivitye",
)

# CDC mode using auto_cdc
dp.create_auto_cdc_from_snapshot_flow(
    target=TABLE_ANALYTICS_ENVRACTIVITY,
    source="v_join_environmntalactivity",
    keys=["FARM_NO","SHED_NO","DAY","HOUR"],
    stored_as_scd_type=1,
)

