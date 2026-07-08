"""PySpark session factory."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from src.common.storage import uses_hdfs


def _configure_windows_hadoop() -> None:
    if sys.platform != "win32":
        return
    root = Path(__file__).resolve().parents[2]
    hadoop_home = root / "tools" / "hadoop"
    bin_dir = hadoop_home / "bin"
    if bin_dir.exists():
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        os.environ["hadoop.home.dir"] = str(hadoop_home)
        os.environ["PATH"] = str(bin_dir) + os.pathsep + os.environ.get("PATH", "")
        core_site = hadoop_home / "etc" / "hadoop" / "core-site.xml"
        if core_site.exists():
            os.environ["HADOOP_CONF_DIR"] = str(core_site.parent)


def create_spark_session(config: dict[str, Any], num_executors: int | None = None) -> SparkSession:
    _configure_windows_hadoop()
    spark_cfg = config.get("spark", {})
    executors = num_executors or config.get("spark_num_executors", 2)
    master = spark_cfg.get("master", "local[*]")

    builder = (
        SparkSession.builder.appName(spark_cfg.get("app_name", "NYC-Taxi-Lakehouse"))
        .master(master)
        .config("spark.driver.memory", spark_cfg.get("driver_memory", "4g"))
        .config("spark.executor.memory", spark_cfg.get("executor_memory", "4g"))
        .config("spark.sql.shuffle.partitions", str(spark_cfg.get("shuffle_partitions", 8)))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.hadoop.io.native.lib.available", "false")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
    )

    if executors and master.startswith("local"):
        builder = builder.config("spark.executor.instances", str(executors))

    if config.get("light_mode"):
        builder = (
            builder.config("spark.sql.autoBroadcastJoinThreshold", "-1")
            .config("spark.memory.fraction", "0.5")
            .config("spark.memory.storageFraction", "0.3")
        )

    if config.get("docker_mode"):
        builder = builder.config("spark.executor.instances", str(executors))
        if not uses_hdfs(config):
            builder = builder.config("spark.hadoop.fs.defaultFS", "file:///")

    return builder.getOrCreate()
