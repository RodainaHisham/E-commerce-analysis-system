import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

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

# ─── dim_date ─────────────────────────────────────────────────────────────────
print("\n--- Building dim_date ---")

order_dates      = stream_orders.select(F.to_date(F.col("event_time")).alias("date"))
event_dates      = stream_events.select(F.to_date(F.col("event_time")).alias("date"))
review_dates     = stream_reviews.select(F.to_date(F.col("event_time")).alias("date"))
order_item_dates = stream_order_items.select(F.to_date(F.col("event_time")).alias("date"))

dim_date = order_dates \
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
    ) \
    .cache()

dim_date.orderBy("date_id") \
    .write.mode("overwrite").parquet(GOLD_PATH + "dim_date/")

# ─── dim_order_status ─────────────────────────────────────────────────────────
print("\n--- Building dim_order_status ---")

dim_order_status = stream_orders \
    .select("order_status") \
    .dropDuplicates() \
    .filter(F.col("order_status").isNotNull()) \
    .withColumn("status_id", F.monotonically_increasing_id()) \
    .select("status_id", "order_status") \
    .cache()

dim_order_status.write.mode("overwrite").parquet(GOLD_PATH + "dim_order_status/")

# ─── dim_event_type ───────────────────────────────────────────────────────────
print("\n--- Building dim_event_type ---")

dim_event_type = stream_events \
    .select("event_type") \
    .dropDuplicates() \
    .filter(F.col("event_type").isNotNull()) \
    .withColumn("event_type_id", F.monotonically_increasing_id()) \
    .select("event_type_id", F.col("event_type").alias("event_type_name")) \
    .cache()

dim_event_type.write.mode("overwrite").parquet(GOLD_PATH + "dim_event_type/")

# ══════════════════════════════════════════════════════════════════════════════
# FACT TABLES
# ══════════════════════════════════════════════════════════════════════════════

# ─── fact_orders ──────────────────────────────────────────────────────────────
# The join on "order_status" brings in status_id.
# The final .select() deliberately omits "order_status" — only status_id is kept.
# "order_status" is still readable inside the select expression for is_returned.
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
    F.col("order_status")   # used to join → status_id; excluded from final select
) \
.filter(F.col("order_id").isNotNull())

# Intermediate joined df: has both order_status (string) AND status_id (int)
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

# Final select: status_id IN, order_status OUT
fact_orders = joined_orders.select(
    F.col("order_item_id"),
    F.col("order_id"),
    F.col("user_id"),
    F.col("product_id"),
    F.col("order_date_id"),
    F.col("status_id"),                                              # FK only
    F.col("order_timestamp"),
    F.col("quantity"),
    F.col("item_price"),
    F.col("list_price"),
    F.round(F.col("item_price") * F.col("quantity"), 2).alias("line_total"),
    F.col("total_amount"),
    F.when(F.col("order_status") == "Returned", 1)
     .otherwise(0).alias("is_returned"),                            # derived then dropped
    F.current_timestamp().alias("gold_created_at")
    # "order_status" string column is NOT listed → absent from parquet schema
)

fact_orders \
    .repartition(4, F.col("order_date_id")) \
    .write.mode("overwrite") \
    .partitionBy("order_date_id") \
    .parquet(GOLD_PATH + "fact_orders/")

print("fact_orders columns:", spark.read.parquet(GOLD_PATH + "fact_orders/").columns)

# ─── fact_events ──────────────────────────────────────────────────────────────
# The join brings in event_type_id.
# The final .select() deliberately omits "event_type" / "event_type_name".
print("\n--- Building fact_events ---")

# Intermediate joined df: has both event_type (string) AND event_type_id (int)
joined_events = stream_events \
    .join(
        dim_event_type,
        on=stream_events["event_type"] == dim_event_type["event_type_name"],
        how="left"
    )

# Final select: event_type_id IN, event_type string OUT
fact_events = joined_events.select(
    F.col("event_id"),
    F.col("user_id"),
    F.col("product_id"),
    F.to_date("event_time").alias("event_date_id"),
    F.col("event_type_id"),                                         # FK only
    F.col("event_type_id").alias("funnel_step"),
    F.to_timestamp("event_time").alias("event_timestamp"),
    F.current_timestamp().alias("gold_created_at")
    # "event_type" / "event_type_name" strings are NOT listed → absent from parquet schema
) \
.filter(
    F.col("event_id").isNotNull() &
    F.col("user_id").isNotNull() &
    F.col("product_id").isNotNull()
)

fact_events \
    .repartition(4, F.col("event_date_id")) \
    .write.mode("overwrite") \
    .partitionBy("event_date_id") \
    .parquet(GOLD_PATH + "fact_events/")

print("fact_events columns:", spark.read.parquet(GOLD_PATH + "fact_events/").columns)

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