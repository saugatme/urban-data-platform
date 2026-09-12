"""
Task 6 — Benchmark Two Storage Strategies for Taxi Trips

Strategy A: Partitioned by (year, month)
Strategy B: No partitioning (flat Delta table)

Measures:
- Ingestion time
- Storage size (bytes + file count)
- Query latency for 3 analytical queries
"""

import time
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, unix_timestamp



STRATEGY_A = "data/benchmark/strategy_a_partitioned"
STRATEGY_B = "data/benchmark/strategy_b_flat"



def ingest_strategy_a(spark: SparkSession) -> float:
    """Partitioned by year, month."""
    df = spark.read.format("delta").load("data/silver/taxi_trips")
    t0 = time.time()
    df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .save(STRATEGY_A)
    return time.time() - t0


def ingest_strategy_b(spark: SparkSession) -> float:
    """No partitioning — flat Delta table."""
    df = spark.read.format("delta").load("data/silver/taxi_trips")
    t0 = time.time()
    df.write.format("delta") \
        .mode("overwrite") \
        .save(STRATEGY_B)
    return time.time() - t0



def storage_metrics(path: str) -> dict:
    """Count files and total size in bytes under path."""
    total_size  = 0
    total_files = 0
    parquet_files = 0
    for root, dirs, files in os.walk(path):
        dirs[:] = [d for d in dirs if d != "_delta_log"]
        for f in files:
            if not f.startswith("."):
                fp = os.path.join(root, f)
                total_size += os.path.getsize(fp)
                total_files += 1
                if f.endswith(".parquet"):
                    parquet_files += 1
    return {
        "total_size_mb": round(total_size / 1024 / 1024, 1),
        "total_files":   total_files,
        "parquet_files": parquet_files,
    }



def query_trips_per_borough(spark: SparkSession, path: str) -> float:
    """Q1: Number of taxi trips per pickup borough."""
    zones = spark.read.format("delta").load("data/silver/taxi_zones")
    df    = spark.read.format("delta").load(path)
    t0 = time.time()
    result = df.join(zones, df["pickup_location_id"] == zones["location_id"], "left") \
               .groupBy("borough") \
               .agg(count("*").alias("trip_count")) \
               .orderBy(col("trip_count").desc())
    result.collect()
    return time.time() - t0


def query_avg_duration_per_day(spark: SparkSession, path: str) -> float:
    """Q2: Average trip duration (minutes) per day."""
    from pyspark.sql.functions import to_date, round as spark_round
    df = spark.read.format("delta").load(path)
    t0 = time.time()
    result = df.withColumn("pickup_date", to_date(col("pickup_datetime"))) \
               .withColumn("duration_min",
                   (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60) \
               .groupBy("pickup_date") \
               .agg(spark_round(avg("duration_min"), 2).alias("avg_duration_min")) \
               .orderBy("pickup_date")
    result.collect()
    return time.time() - t0


def query_avg_fare_per_borough(spark: SparkSession, path: str) -> float:
    """Q3: Average fare amount per pickup borough."""
    from pyspark.sql.functions import round as spark_round
    zones = spark.read.format("delta").load("data/silver/taxi_zones")
    df    = spark.read.format("delta").load(path)
    t0 = time.time()
    result = df.join(zones, df["pickup_location_id"] == zones["location_id"], "left") \
               .groupBy("borough") \
               .agg(spark_round(avg("fare_amount"), 2).alias("avg_fare")) \
               .orderBy("borough")
    result.collect()
    return time.time() - t0



def benchmark_queries(spark: SparkSession, path: str, label: str) -> dict:
    print(f"\n  Warming up {label}...")
    query_trips_per_borough(spark, path)
    query_avg_duration_per_day(spark, path)
    query_avg_fare_per_borough(spark, path)

    print(f"  Measuring {label}...")
    q1 = query_trips_per_borough(spark, path)
    q2 = query_avg_duration_per_day(spark, path)
    q3 = query_avg_fare_per_borough(spark, path)
    return {"q1_trips_per_borough": round(q1, 2),
            "q2_avg_duration_per_day": round(q2, 2),
            "q3_avg_fare_per_borough": round(q3, 2)}



def print_report(results: dict):
    print("\n" + "="*60)
    print("BENCHMARK REPORT")
    print("="*60)

    a, b = results["strategy_a"], results["strategy_b"]

    print(f"\n{'Metric':<30} {'Strategy A':>15} {'Strategy B':>15}")
    print(f"{'':.<30} {'(year/month)':>15} {'(flat)':>15}")
    print("-"*60)
    print(f"{'Ingestion time (s)':<30} {a['ingestion_s']:>15} {b['ingestion_s']:>15}")
    print(f"{'Storage size (MB)':<30} {a['storage']['total_size_mb']:>15} {b['storage']['total_size_mb']:>15}")
    print(f"{'Parquet files':<30} {a['storage']['parquet_files']:>15} {b['storage']['parquet_files']:>15}")
    print(f"{'Q1: trips per borough (s)':<30} {a['queries']['q1_trips_per_borough']:>15} {b['queries']['q1_trips_per_borough']:>15}")
    print(f"{'Q2: avg duration/day (s)':<30} {a['queries']['q2_avg_duration_per_day']:>15} {b['queries']['q2_avg_duration_per_day']:>15}")
    print(f"{'Q3: avg fare/borough (s)':<30} {a['queries']['q3_avg_fare_per_borough']:>15} {b['queries']['q3_avg_fare_per_borough']:>15}")
    print("="*60)



def run_benchmark(spark: SparkSession):
    results = {}

    print("\n>>> Strategy A: Partitioned by (year, month)")
    t_a = ingest_strategy_a(spark)
    print(f"  Ingestion: {t_a:.1f}s")
    s_a = storage_metrics(STRATEGY_A)
    print(f"  Storage:   {s_a['total_size_mb']} MB, {s_a['parquet_files']} parquet files")
    q_a = benchmark_queries(spark, STRATEGY_A, "Strategy A")
    results["strategy_a"] = {"ingestion_s": round(t_a, 1), "storage": s_a, "queries": q_a}

    print("\n>>> Strategy B: Flat (no partitioning)")
    t_b = ingest_strategy_b(spark)
    print(f"  Ingestion: {t_b:.1f}s")
    s_b = storage_metrics(STRATEGY_B)
    print(f"  Storage:   {s_b['total_size_mb']} MB, {s_b['parquet_files']} parquet files")
    q_b = benchmark_queries(spark, STRATEGY_B, "Strategy B")
    results["strategy_b"] = {"ingestion_s": round(t_b, 1), "storage": s_b, "queries": q_b}

    print_report(results)
    return results
