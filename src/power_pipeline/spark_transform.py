"""PySpark transformation, quality gates and versioned quarterly snapshots."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

NUMERIC_COLUMNS = [
    "Global_active_power",
    "Global_reactive_power",
    "Voltage",
    "Global_intensity",
    "Sub_metering_1",
    "Sub_metering_2",
    "Sub_metering_3",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--hdfs-uri", required=True)
    parser.add_argument("--max-null-ratio", type=float, default=0.05)
    return parser.parse_args()


def _filesystem(spark: SparkSession):
    jvm = spark._jvm
    return jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())


def _path(spark: SparkSession, value: str):
    return spark._jvm.org.apache.hadoop.fs.Path(value)


def atomic_write_parquet(
    spark: SparkSession, dataframe: DataFrame, *, stage_path: str, final_path: str
) -> str:
    fs = _filesystem(spark)
    stage = _path(spark, stage_path)
    final = _path(spark, final_path)
    if fs.exists(final):
        return "already_exists"
    if fs.exists(stage):
        fs.delete(stage, True)
    fs.mkdirs(final.getParent())
    dataframe.write.mode("errorifexists").parquet(stage_path)
    if not fs.rename(stage, final):
        fs.delete(stage, True)
        raise RuntimeError(f"Atomic HDFS rename failed: {stage_path} -> {final_path}")
    return "published"


def atomic_write_json(
    spark: SparkSession, payload: dict, *, stage_path: str, final_path: str
) -> str:
    record = spark.createDataFrame([(json.dumps(payload, sort_keys=True),)], ["value"])
    fs = _filesystem(spark)
    stage = _path(spark, stage_path)
    final = _path(spark, final_path)
    if fs.exists(final):
        return "already_exists"
    if fs.exists(stage):
        fs.delete(stage, True)
    fs.mkdirs(final.getParent())
    record.coalesce(1).write.mode("errorifexists").text(stage_path)
    if not fs.rename(stage, final):
        fs.delete(stage, True)
        raise RuntimeError(f"Atomic HDFS rename failed: {stage_path} -> {final_path}")
    return "published"


def typed_source(spark: SparkSession, raw_path: str) -> DataFrame:
    schema = T.StructType(
        [
            T.StructField("Date", T.StringType(), False),
            T.StructField("Time", T.StringType(), False),
            *[T.StructField(column, T.StringType(), True) for column in NUMERIC_COLUMNS],
        ]
    )
    source = (
        spark.read.option("header", True)
        .option("sep", ";")
        .option("mode", "FAILFAST")
        .schema(schema)
        .csv(raw_path)
    )
    typed = source.withColumn(
        "timestamp",
        F.to_timestamp(F.concat_ws(" ", F.col("Date"), F.col("Time")), "dd/MM/yyyy HH:mm:ss"),
    )
    for column in NUMERIC_COLUMNS:
        typed = typed.withColumn(
            column,
            F.when(F.col(column).isin("?", ""), None)
            .otherwise(F.col(column))
            .cast("double"),
        )
    return typed.drop("Date", "Time")


def validate_and_clean(
    dataframe: DataFrame, *, year: int, month: int, max_null_ratio: float
) -> tuple[DataFrame, dict]:
    total_rows = dataframe.count()
    if total_rows == 0:
        raise ValueError("Raw batch is empty")
    invalid_timestamps = dataframe.filter(F.col("timestamp").isNull()).count()
    out_of_period = dataframe.filter(
        (F.year("timestamp") != year) | (F.month("timestamp") != month)
    ).count()
    distinct_timestamps = dataframe.select("timestamp").distinct().count()
    duplicate_timestamps = total_rows - distinct_timestamps
    missing_measurements = dataframe.filter(
        F.col("Global_active_power").isNull()
    ).count()
    null_ratio = missing_measurements / total_rows
    negative_active_power = dataframe.filter(F.col("Global_active_power") < 0).count()

    metrics = {
        "total_rows": total_rows,
        "invalid_timestamps": invalid_timestamps,
        "out_of_period_rows": out_of_period,
        "duplicate_timestamps": duplicate_timestamps,
        "missing_global_active_power_rows": missing_measurements,
        "missing_global_active_power_ratio": null_ratio,
        "negative_global_active_power_rows": negative_active_power,
        "max_allowed_null_ratio": max_null_ratio,
    }
    failures = {
        "invalid_timestamps": invalid_timestamps,
        "out_of_period_rows": out_of_period,
        "duplicate_timestamps": duplicate_timestamps,
        "negative_global_active_power_rows": negative_active_power,
    }
    if any(failures.values()) or null_ratio > max_null_ratio:
        raise ValueError(f"Curated quality gate failed: {json.dumps(metrics, sort_keys=True)}")

    cleaned = dataframe.dropDuplicates(["timestamp"]).withColumn(
        "is_missing_measurement",
        F.col("Global_active_power").isNull(),
    )
    prior_rows = Window.orderBy("timestamp").rowsBetween(Window.unboundedPreceding, -1)
    for column in NUMERIC_COLUMNS:
        cleaned = cleaned.withColumn(
            f"{column}_filled",
            F.coalesce(F.col(column), F.last(F.col(column), ignorenulls=True).over(prior_rows)),
        )
    return cleaned, metrics


def aggregate_hourly(cleaned: DataFrame) -> DataFrame:
    aggregations = []
    for column in NUMERIC_COLUMNS:
        filled = F.col(f"{column}_filled")
        aggregations.extend(
            [
                F.avg(filled).alias(f"{column.lower()}_mean"),
                F.min(filled).alias(f"{column.lower()}_min"),
                F.max(filled).alias(f"{column.lower()}_max"),
            ]
        )
    aggregations.extend(
        [
            F.stddev_pop("Global_active_power_filled").alias(
                "global_active_power_stddev"
            ),
            (F.sum("Global_active_power_filled") / F.lit(60.0)).alias(
                "energy_consumption_kwh"
            ),
            F.sum(F.col("is_missing_measurement").cast("int")).alias("missing_minutes"),
            F.count("timestamp").alias("observed_minutes"),
        ]
    )
    return (
        cleaned.withColumn("period_start", F.date_trunc("hour", "timestamp"))
        .groupBy("period_start")
        .agg(*aggregations)
        .withColumn("year", F.year("period_start"))
        .withColumn("month", F.month("period_start"))
        .orderBy("period_start")
    )


def aggregate_daily(cleaned: DataFrame) -> DataFrame:
    return (
        cleaned.withColumn("period_start", F.to_date("timestamp"))
        .groupBy("period_start")
        .agg(
            F.avg("Global_active_power_filled").alias("global_active_power_mean"),
            F.min("Global_active_power_filled").alias("global_active_power_min"),
            F.max("Global_active_power_filled").alias("global_active_power_max"),
            F.stddev_pop("Global_active_power_filled").alias(
                "global_active_power_stddev"
            ),
            (F.sum("Global_active_power_filled") / F.lit(60.0)).alias(
                "energy_consumption_kwh"
            ),
            F.sum(F.col("is_missing_measurement").cast("int")).alias("missing_minutes"),
            F.count("timestamp").alias("observed_minutes"),
        )
        .withColumn("year", F.year("period_start"))
        .withColumn("month", F.month("period_start"))
        .orderBy("period_start")
    )


def build_features(hourly: DataFrame) -> DataFrame:
    ordered = Window.orderBy("period_start")
    trailing = ordered.rowsBetween(-24, -1)
    return (
        hourly.withColumn("hour", F.hour("period_start"))
        .withColumn("day_of_week", F.dayofweek("period_start"))
        .withColumn("is_weekend", F.dayofweek("period_start").isin(1, 7).cast("boolean"))
        .withColumn(
            "global_active_power_lag_1h",
            F.lag("global_active_power_mean", 1).over(ordered),
        )
        .withColumn(
            "global_active_power_lag_24h",
            F.lag("global_active_power_mean", 24).over(ordered),
        )
        .withColumn(
            "global_active_power_rolling_mean_24h",
            F.avg("global_active_power_mean").over(trailing),
        )
    )


def run(args: argparse.Namespace) -> dict:
    spark = (
        SparkSession.builder.appName(f"power-transform-{args.year:04d}-{args.month:02d}")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    base = args.hdfs_uri.rstrip("/")
    raw_path = (
        f"{base}/data/raw/source=uci/year={args.year:04d}/month={args.month:02d}/"
        f"batch_id={args.batch_id}/data.csv"
    )
    stage_root = f"{base}/data/_staging/spark/{args.batch_id}"
    hourly_path = (
        f"{base}/data/curated/hourly/year={args.year:04d}/month={args.month:02d}/"
        f"batch_id={args.batch_id}"
    )
    daily_path = (
        f"{base}/data/curated/daily/year={args.year:04d}/month={args.month:02d}/"
        f"batch_id={args.batch_id}"
    )

    typed = typed_source(spark, raw_path)
    cleaned, quality = validate_and_clean(
        typed,
        year=args.year,
        month=args.month,
        max_null_ratio=args.max_null_ratio,
    )
    hourly = aggregate_hourly(cleaned).cache()
    daily = aggregate_daily(cleaned)
    hourly_rows = hourly.count()
    daily_rows = daily.count()
    if hourly_rows < 24 or daily_rows < 1:
        raise ValueError("Aggregated output is unexpectedly small")

    outputs = {
        "hourly": {
            "path": hourly_path,
            "status": atomic_write_parquet(
                spark,
                hourly,
                stage_path=f"{stage_root}/hourly",
                final_path=hourly_path,
            ),
            "rows": hourly_rows,
        },
        "daily": {
            "path": daily_path,
            "status": atomic_write_parquet(
                spark,
                daily,
                stage_path=f"{stage_root}/daily",
                final_path=daily_path,
            ),
            "rows": daily_rows,
        },
    }

    if args.month in {3, 6, 9, 12}:
        quarter = ((args.month - 1) // 3) + 1
        year_hourly = f"{base}/data/curated/hourly/year={args.year:04d}/*/batch_id=*"
        quarter_hourly = spark.read.parquet(year_hourly).filter(
            (F.month("period_start") >= (quarter - 1) * 3 + 1)
            & (F.month("period_start") <= quarter * 3)
        )
        features = build_features(quarter_hourly).orderBy("period_start")
        feature_rows = features.count()
        if feature_rows == 0:
            raise ValueError("Quarterly feature snapshot would be empty")
        snapshot_path = (
            f"{base}/data/ml_ready/snapshot={args.year:04d}-Q{quarter}/"
            f"version={args.batch_id}"
        )
        outputs["snapshot"] = {
            "path": snapshot_path,
            "status": atomic_write_parquet(
                spark,
                features,
                stage_path=f"{stage_root}/snapshot",
                final_path=snapshot_path,
            ),
            "rows": feature_rows,
        }

    report = {
        "schema_version": "1.0",
        "batch_id": args.batch_id,
        "year": args.year,
        "month": args.month,
        # Official Spark 3.5.7 image uses Python 3.10; datetime.UTC is unavailable there.
        "processed_at_utc": datetime.now(timezone.utc).isoformat(),  # noqa: UP017
        "spark_application_id": spark.sparkContext.applicationId,
        "git_commit": os.getenv("GIT_COMMIT", "unknown"),
        "raw_path": raw_path,
        "quality": quality,
        "outputs": outputs,
        "missing_value_rule": (
            "Keep nulls in typed data; for aggregates use only the last previously observed "
            "non-null value. Future values are never used."
        ),
    }
    report_path = f"{base}/data/governance/batches/{args.batch_id}"
    atomic_write_json(
        spark,
        report,
        stage_path=f"{stage_root}/governance",
        final_path=report_path,
    )
    print(json.dumps(report, sort_keys=True))
    spark.stop()
    return report


if __name__ == "__main__":
    run(parse_args())
