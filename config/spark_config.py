"""
FinAiMine-Data — Spark Session Factory & Delta Lake Storage Helpers
"""

import os
import sys
import logging
from typing import Optional
from pathlib import Path
import pandas as pd

from config.settings import (
    DELTA_WAREHOUSE_PATH,
    SPARK_MASTER,
    SPARK_DRIVER_MEMORY,
)

logger = logging.getLogger(__name__)

_spark_session = None


def get_spark():
    """
    Creates or returns existing PySpark SparkSession configured with Delta Lake support.
    """
    global _spark_session
    if _spark_session is not None:
        return _spark_session

    try:
        from pyspark.sql import SparkSession
        from delta import configure_spark_with_delta_pip

        builder = (
            SparkSession.builder
            .appName("FinAIMine")
            .master(SPARK_MASTER)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalogExtension")
            .config("spark.driver.memory", SPARK_DRIVER_MEMORY)
            .config("spark.sql.warehouse.dir", str(DELTA_WAREHOUSE_PATH))
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
        )
        _spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
        return _spark_session
    except Exception as e:
        logger.warning("Could not initialize full PySpark session (%s). Delta table operations will fallback to deltalake/pandas.", e)
        return None


def write_delta_table(df: pd.DataFrame, table_path: str, mode: str = "overwrite", partition_by: Optional[list] = None):
    """
    Writes a Pandas DataFrame to a Delta Lake table at table_path.
    Uses PySpark if available; otherwise uses native 'deltalake' (Rust/Python engine)
    which works seamlessly on Windows without requiring Hadoop winutils.
    """
    Path(table_path).mkdir(parents=True, exist_ok=True)
    spark = None
    try:
        spark = get_spark()
    except Exception:
        pass

    if spark is not None:
        try:
            spark_df = spark.createDataFrame(df)
            writer = spark_df.write.format("delta").mode(mode)
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            writer.save(table_path)
            return
        except Exception as spark_err:
            logger.info("Spark Delta write encountered error (%s), using native deltalake engine...", spark_err)

    # Native deltalake write fallback
    try:
        from deltalake import write_deltalake
        write_deltalake(
            table_path,
            df,
            mode=mode,
            partition_by=partition_by or []
        )
    except Exception as dl_err:
        logger.info("Native deltalake fallback: saving as parquet/csv at %s (%s)", table_path, dl_err)
        try:
            parquet_file = Path(table_path) / "data.parquet"
            df.to_parquet(parquet_file, index=False)
        except Exception:
            csv_file = Path(table_path) / "data.csv"
            df.to_csv(csv_file, index=False)


def read_delta_table(table_path: str) -> pd.DataFrame:
    """
    Reads a Delta Lake table into a Pandas DataFrame.
    Tries native deltalake, PySpark, Parquet, or CSV fallback.
    """
    p = Path(table_path)
    if not p.exists() or not any(p.iterdir()):
        return pd.DataFrame()

    # 1. Try native deltalake
    try:
        from deltalake import DeltaTable
        dt = DeltaTable(table_path)
        return dt.to_pandas()
    except Exception:
        pass

    # 2. Try PySpark
    spark = None
    try:
        spark = get_spark()
    except Exception:
        pass

    if spark is not None:
        try:
            return spark.read.format("delta").load(table_path).toPandas()
        except Exception:
            pass

    # 3. Check for parquet files
    parquet_files = list(p.glob("**/*.parquet"))
    if parquet_files:
        try:
            dfs = [pd.read_parquet(f) for f in parquet_files if not f.name.startswith(".")]
            if dfs:
                return pd.concat(dfs, ignore_index=True)
        except Exception:
            pass

    # 4. Check for csv files
    csv_files = list(p.glob("**/*.csv"))
    if csv_files:
        try:
            dfs = [pd.read_csv(f) for f in csv_files if not f.name.startswith(".")]
            if dfs:
                return pd.concat(dfs, ignore_index=True)
        except Exception:
            pass

    return pd.DataFrame()

