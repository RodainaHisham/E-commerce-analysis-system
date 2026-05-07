import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from pyspark.sql.functions import col, explode

os.environ["HADOOP_USER_NAME"] = "root"
os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3.11"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3.11"

spark = SparkSession.builder \
    .appName('EcommerceAnalysisSystem_Bronze') \
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

STREAM_PATH = "hdfs://hadoop-namenode:9000/user/jovyan/raw_ecommerce/stream/"
STATIC_PATH = "hdfs://hadoop-namenode:9000/user/jovyan/raw_ecommerce/static/"
BRONZE_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/bronze/ecommerce/"

# ── Nested schema matching actual JSON structure ──────────────────────────────
stream_schema = StructType([
    StructField("stream_type",    StringType(), True),
    StructField("event_time",     StringType(), True),
    StructField("ingestion_time", StringType(), True),
    StructField("data", StructType([
        StructField("event_id",      StringType(),  True),
        StructField("user_id",       StringType(),  True),
        StructField("product_id",    StringType(),  True),
        StructField("event_type",    StringType(),  True),
        StructField("order_id",      StringType(),  True),
        StructField("total_amount",  DoubleType(),  True),
        StructField("order_status",  StringType(),  True),
        StructField("order_item_id", StringType(),  True),
        StructField("quantity",      IntegerType(), True),
        StructField("item_price",    DoubleType(),  True),
        StructField("review_id",     StringType(),  True),
        StructField("rating",        DoubleType(),  True),
        StructField("review_text",   StringType(),  True),
    ]), True)
])

users_schema = StructType([
    StructField("user_id",  StringType(),  True),
    StructField("name",     StringType(),  True),
    StructField("email",    StringType(),  True),
    StructField("city",  StringType(),  True),
    StructField("gender",   StringType(), True)
])

products_schema = StructType([
    StructField("product_id",   StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category",     StringType(), True),
    StructField("price",        DoubleType(), True),
    StructField("brand",        StringType(), True)
])

# ── Stream ingestion ──────────────────────────────────────────────────────────
try:
    print("\nReading stream data...")

    # The file is a JSON array — use multiLine mode to parse it correctly
    raw_df = spark.read.option("multiLine", "true").schema(stream_schema).json(STREAM_PATH)

    # If the top level is an array, explode it first
    # Check whether the root is ArrayType by inspecting the first field name
    if raw_df.schema.fields[0].name == "stream_type":
        # Already a flat array of records — no explode needed
        nested_df = raw_df
    else:
        # Root is a single array field — explode it
        array_field = raw_df.schema.fields[0].name
        nested_df = raw_df.select(explode(col(array_field)).alias("rec")) \
                          .select("rec.*")


    # Flatten the nested `data` struct into top-level columns
    flat_stream = nested_df.select(
        "stream_type", "event_time", "ingestion_time",
        col("data.event_id").alias("event_id"),
        col("data.user_id").alias("user_id"),
        col("data.product_id").alias("product_id"),
        col("data.event_type").alias("event_type"),
        col("data.order_id").alias("order_id"),
        col("data.total_amount").alias("total_amount"),
        col("data.order_status").alias("order_status"),
        col("data.order_item_id").alias("order_item_id"),
        col("data.quantity").alias("quantity"),
        col("data.item_price").alias("item_price"),
        col("data.review_id").alias("review_id"),
        col("data.rating").alias("rating"),
        col("data.review_text").alias("review_text"),
    )

    total = flat_stream.count()
    print(f"Total stream records: {total}")
    flat_stream.show(5, truncate=False)

    print("\nstream_type distribution:")
    flat_stream.groupBy("stream_type").count().orderBy("stream_type").show(truncate=False)

    if total > 0:
        flat_stream.write.mode("overwrite") \
            .partitionBy("stream_type") \
            .parquet(BRONZE_PATH + "stream/")
        print("Stream data written to Bronze.")
    else:
        print("WARNING: 0 records after flattening — check multiLine/explode logic.")

except Exception as e:
    print(f"Stream ingestion error: {e}")
    import traceback; traceback.print_exc()

# ── Static ingestion ──────────────────────────────────────────────────────────
try:
    print("\nReading static users...")
    users_df = spark.read.option("multiLine", "true").schema(users_schema).json(STATIC_PATH + "users.json")
    u_count = users_df.count()
    print(f"Users read: {u_count}")
    users_df.show(5, truncate=False)
    if u_count > 0:
        users_df.write.mode("overwrite").parquet(BRONZE_PATH + "stat_users/")
        print("Users written to Bronze.")
    else:
        print("WARNING: users.json empty or schema mismatch.")
except Exception as e:
    print(f"Users ingestion error: {e}")
    import traceback; traceback.print_exc()

try:
    print("\nReading static products...")
    products_df = spark.read.option("multiLine", "true").schema(products_schema).json(STATIC_PATH + "products.json")
    p_count = products_df.count()
    print(f"Products read: {p_count}")
    products_df.show(5, truncate=False)
    if p_count > 0:
        products_df.write.mode("overwrite").parquet(BRONZE_PATH + "stat_products/")
        print("Products written to Bronze.")
    else:
        print("WARNING: products.json empty or schema mismatch.")
except Exception as e:
    print(f"Products ingestion error: {e}")
    import traceback; traceback.print_exc()

spark.stop()
print("\nSpark Session Stopped")
