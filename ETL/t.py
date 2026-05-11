import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from datetime import date

os.environ["HADOOP_USER_NAME"]      = "root"
os.environ["PYSPARK_PYTHON"]        = "/usr/local/bin/python3.11"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3.11"

spark = SparkSession.builder \
    .appName('EcommerceAnalysisSystem_Gold') \
    .master('yarn') \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode:9000") \
    .config("spark.hadoop.yarn.resourcemanager.hostname", "resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address", "resourcemanager:8032") \
    .config("spark.hadoop.yarn.resourcemanager.scheduler.address", "resourcemanager:8030") \
    .config("spark.executor.memory", "512m") \
    .config("spark.yarn.am.memory", "512m") \
    .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/usr/local/bin/python3.11") \
    .config("spark.executorEnv.PYSPARK_PYTHON", "/usr/local/bin/python3.11") \
    .getOrCreate()

print("Spark Connected Successfully")

BRONZE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/ecommerce/"
GOLD_PATH   = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/ecommerce/"

TODAY = str(date.today())
print(f"Processing date: {TODAY}")

# ══════════════════════════════════════════════════════════════════════════════
# READ BRONZE LAYER
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== Reading Bronze Layer ===")

stream_all = spark.read.parquet(BRONZE_PATH + "stream/").cache()

stream_events      = stream_all.filter(F.col("stream_type") == "events")
stream_orders      = stream_all.filter(F.col("stream_type") == "orders")
stream_order_items = stream_all.filter(F.col("stream_type") == "order_items")
stream_reviews     = stream_all.filter(F.col("stream_type") == "reviews")

bronze_users    = spark.read.parquet(BRONZE_PATH + "stat_users/")
bronze_products = spark.read.parquet(BRONZE_PATH + "stat_products/")

print("Bronze tables loaded.")

# ══════════════════════════════════════════════════════════════════════════════
# DIMENSION TABLES
# ══════════════════════════════════════════════════════════════════════════════

# ─── dim_users ────────────────────────────────────────────────────────────────
print("\n--- Building dim_users ---")

dim_users = bronze_users.select(
    F.col("user_id"),
    F.col("name"),
    F.col("email"),
    F.col("gender"),
    F.col("city")
) \
.dropDuplicates(["user_id"]) \
.filter(F.col("user_id").isNotNull()) \
.cache()

dim_users.write.mode("overwrite").parquet(GOLD_PATH + "dim_users/")
print(f"dim_users written: {dim_users.count()} rows")

# ─── dim_products ─────────────────────────────────────────────────────────────
print("\n--- Building dim_products ---")

avg_ratings = stream_reviews \
    .filter(F.col("rating").isNotNull()) \
    .groupBy("product_id") \
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("review_id").alias("review_count")
    )

dim_products = bronze_products.select(
    F.col("product_id"),
    F.col("product_name"),
    F.col("category"),
    F.col("price").alias("list_price"),
    F.col("brand")
) \
.dropDuplicates(["product_id"]) \
.filter(F.col("product_id").isNotNull()) \
.join(avg_ratings, on="product_id", how="left") \
.withColumn("avg_rating",   F.coalesce(F.col("avg_rating"),   F.lit(0.0))) \
.withColumn("review_count", F.coalesce(F.col("review_count"), F.lit(0))) \
.cache()

dim_products.write.mode("overwrite").parquet(GOLD_PATH + "dim_products/")
print(f"dim_products written: {dim_products.count()} rows")

# ─── dim_date ─────────────────────────────────────────────────────────────────
print("\n--- Building dim_date ---")

order_dates      = stream_orders.select(F.to_date(F.col("event_time")).alias("date"))
event_dates      = stream_events.select(F.to_date(F.col("event_time")).alias("date"))
review_dates     = stream_reviews.select(F.to_date(F.col("event_time")).alias("date"))
order_item_dates = stream_order_items.select(F.to_date(F.col("event_time")).alias("date"))

dim_date_new = order_dates \
    .union(event_dates) \
    .union(review_dates) \
    .union(order_item_dates) \
    .dropDuplicates(["date"]) \
    .filter(
        F.col("date").isNotNull() &
        F.col("date").between("2023-01-01", "2026-12-31")
    ) \
    .select(
        F.col("date").alias("date_id"),
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.dayofmonth("date").alias("day"),
        F.quarter("date").alias("quarter"),
        F.dayofweek("date").alias("day_of_week"),
        F.date_format("date", "EEEE").alias("day_name"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.when(F.dayofweek("date").isin(1, 7), True)
         .otherwise(False).alias("is_weekend")
    )

dim_date_path = GOLD_PATH + "dim_date/"
try:
    existing_dim_date = spark.read.parquet(dim_date_path)
    dim_date = existing_dim_date \
        .join(dim_date_new, on="date_id", how="left_anti") \
        .union(dim_date_new) \
        .cache()
    print("dim_date: merged with existing")
except Exception:
    dim_date = dim_date_new.cache()
    print("dim_date: first run, writing fresh")

dim_date.orderBy("date_id").write.mode("overwrite").parquet(dim_date_path)
print(f"dim_date written: {dim_date.count()} rows")

# ─── dim_order_status ─────────────────────────────────────────────────────────
# FIX: replaced monotonically_increasing_id() with F.abs(F.hash()) so status_id
#      is deterministic across runs — foreign keys in fact_orders stay valid.
print("\n--- Building dim_order_status ---")

dim_order_status = stream_orders \
    .select("order_status") \
    .dropDuplicates() \
    .filter(F.col("order_status").isNotNull()) \
    .withColumn("status_id", F.abs(F.hash(F.col("order_status")))) \
    .select("status_id", "order_status") \
    .cache()

dim_order_status.write.mode("overwrite").parquet(GOLD_PATH + "dim_order_status/")
print(f"dim_order_status written: {dim_order_status.count()} rows")

# ─── dim_event_type ───────────────────────────────────────────────────────────
# FIX: same deterministic hash approach as dim_order_status.
print("\n--- Building dim_event_type ---")

dim_event_type = stream_events \
    .select("event_type") \
    .dropDuplicates() \
    .filter(F.col("event_type").isNotNull()) \
    .withColumn("event_type_id", F.abs(F.hash(F.col("event_type")))) \
    .select("event_type_id", F.col("event_type").alias("event_type_name")) \
    .cache()

dim_event_type.write.mode("overwrite").parquet(GOLD_PATH + "dim_event_type/")
print(f"dim_event_type written: {dim_event_type.count()} rows")

# ══════════════════════════════════════════════════════════════════════════════
# FACT TABLES
# FIX: removed .filter(date_col == TODAY) before writing.
#      partitionBy + dynamic overwrite already replaces only today's partition;
#      the pre-write filter was preventing historical partitions from accumulating.
# ══════════════════════════════════════════════════════════════════════════════

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ─── fact_orders ──────────────────────────────────────────────────────────────
print("\n--- Building fact_orders ---")

order_items_clean = stream_order_items.select(
    F.col("order_item_id"),
    F.col("order_id"),
    F.col("product_id"),
    F.col("quantity").cast(IntegerType()),
    F.col("item_price").cast(DoubleType())
) \
.filter(
    F.col("order_id").isNotNull() &
    F.col("product_id").isNotNull()
)

orders_clean = stream_orders.select(
    F.col("order_id"),
    F.col("user_id"),
    F.to_timestamp("event_time").alias("order_timestamp"),
    F.to_date("event_time").alias("order_date_id"),
    F.col("total_amount").cast(DoubleType()),
    F.col("order_status")
) \
.filter(F.col("order_id").isNotNull())

joined_orders = order_items_clean \
    .join(orders_clean, on="order_id", how="inner") \
    .join(
        dim_products.select("product_id", "list_price"),
        on="product_id", how="left"
    ) \
    .join(
        dim_order_status.select("status_id", "order_status"),
        on="order_status", how="left"
    )

fact_orders = joined_orders.select(
    F.col("order_item_id"),
    F.col("order_id"),
    F.col("user_id"),
    F.col("product_id"),
    F.col("order_date_id"),
    F.col("status_id"),
    F.col("order_timestamp"),
    F.col("quantity"),
    F.col("item_price"),
    F.col("list_price"),
    F.round(F.col("item_price") * F.col("quantity"), 2).alias("line_total"),
    F.col("total_amount"),
    F.when(F.col("order_status") == "Returned", 1)
     .otherwise(0).alias("is_returned"),
    F.current_timestamp().alias("gold_created_at")
)
# FIX: no TODAY filter here — dynamic partition overwrite handles isolation

fact_orders \
    .repartition(4, F.col("order_date_id")) \
    .write \
    .mode("overwrite") \
    .partitionBy("order_date_id") \
    .parquet(GOLD_PATH + "fact_orders/")

print("fact_orders columns:", spark.read.parquet(GOLD_PATH + "fact_orders/").columns)
print(f"fact_orders rows written: {fact_orders.count()}")

# ─── fact_events ──────────────────────────────────────────────────────────────
print("\n--- Building fact_events ---")

joined_events = stream_events \
    .join(
        dim_event_type,
        on=stream_events["event_type"] == dim_event_type["event_type_name"],
        how="left"
    )

fact_events = joined_events.select(
    F.col("event_id"),
    F.col("user_id"),
    F.col("product_id"),
    F.to_date("event_time").alias("event_date_id"),
    F.col("event_type_id"),
    F.col("event_type_id").alias("funnel_step"),
    F.to_timestamp("event_time").alias("event_timestamp"),
    F.current_timestamp().alias("gold_created_at")
) \
.filter(
    F.col("event_id").isNotNull() &
    F.col("user_id").isNotNull() &
    F.col("product_id").isNotNull()
)
# FIX: no TODAY filter here — dynamic partition overwrite handles isolation

fact_events \
    .repartition(4, F.col("event_date_id")) \
    .write \
    .mode("overwrite") \
    .partitionBy("event_date_id") \
    .parquet(GOLD_PATH + "fact_events/")

print("fact_events columns:", spark.read.parquet(GOLD_PATH + "fact_events/").columns)
print(f"fact_events rows written: {fact_events.count()}")

# ══════════════════════════════════════════════════════════════════════════════
# VALIDATION SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
print("\n=== Gold Layer Summary ===")

summary = {
    "dim_users":        spark.read.parquet(GOLD_PATH + "dim_users/").count(),
    "dim_products":     spark.read.parquet(GOLD_PATH + "dim_products/").count(),
    "dim_date":         spark.read.parquet(GOLD_PATH + "dim_date/").count(),
    "dim_order_status": spark.read.parquet(GOLD_PATH + "dim_order_status/").count(),
    "dim_event_type":   spark.read.parquet(GOLD_PATH + "dim_event_type/").count(),
    "fact_orders":      spark.read.parquet(GOLD_PATH + "fact_orders/").count(),
    "fact_events":      spark.read.parquet(GOLD_PATH + "fact_events/").count(),
}
for table, count in summary.items():
    print(f"  {table:<20} {count:>8} rows")

# ── Unpersist all caches ───────────────────────────────────────────────────────
stream_all.unpersist()
dim_users.unpersist()
dim_products.unpersist()
dim_date.unpersist()
dim_order_status.unpersist()
dim_event_type.unpersist()

spark.stop()
print("\nSpark Session Stopped")