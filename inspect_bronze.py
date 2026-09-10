from src.common.spark_session import get_spark

DATASETS = ["taxi_trips", "weather", "air_quality", "taxi_zones"]

def inspect(spark, base="data"):
    for name in DATASETS:
        path = f"{base}/bronze/{name}"
        df = spark.read.format("delta").load(path)
        total = df.count()
        print(f"\n{'='*50}")
        print(f"{name}: {total:,} rows, {len(df.columns)} cols")
        print("\nSchema:")
        df.printSchema()
        print("Null counts:")
        from pyspark.sql.functions import col, sum as _sum, isnan, when
        null_counts = df.select([
            _sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
            for c in df.columns
        ]).collect()[0].asDict()
        for col_name, cnt in null_counts.items():
            if cnt > 0:
                print(f"  {col_name}: {cnt:,} nulls ({100*cnt/total:.1f}%)")
        if not any(v > 0 for v in null_counts.values()):
            print("  No nulls found")

    # print metadata log
    print(f"\n{'='*50}")
    print("Ingestion log:")
    spark.read.format("delta").load(f"{base}/metadata/ingestion_log").show(truncate=False)

if __name__ == "__main__":
    spark = get_spark("inspect")
    inspect(spark)
    spark.stop()