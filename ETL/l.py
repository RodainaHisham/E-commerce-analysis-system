import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType
from datetime import date


os.environ["HADOOP_USER_NAME"]      = "root"
os.environ["PYSPARK_PYTHON"]        = "/usr/local/bin/python3.11"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3.11"


spark = SparkSession.builder \
    .appName("EcommerceAnalysisSystem_Load") \
    .master("yarn") \
    .config("spark.hadoop.fs.defaultFS",                           "hdfs://hadoop-namenode:9000") \
    .config("spark.hadoop.yarn.resourcemanager.hostname",          "resourcemanager") \
    .config("spark.hadoop.yarn.resourcemanager.address",           "resourcemanager:8032") \
    .config("spark.hadoop.yarn.resourcemanager.scheduler.address", "resourcemanager:8030") \
    .config("spark.driver.host",        "172.30.1.13") \
    .config("spark.driver.bindAddress", "0.0.0.0") \
    .config("spark.executor.memory", "512m") \
    .config("spark.yarn.am.memory",  "512m") \
    .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/usr/local/bin/python3.11") \
    .config("spark.executorEnv.PYSPARK_PYTHON",       "/usr/local/bin/python3.11") \
    .config("spark.jars.packages",
            "net.snowflake:snowflake-jdbc:3.13.33,"
            "net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.3") \
    .getOrCreate()

print("Spark Connected Successfully")


sf_options = {
    "sfURL":      "MQINFFZ-VP41472.snowflakecomputing.com",
    "sfUser":     "RODAINA",
    "sfPassword": "RodainaHisham1102005",
    "sfDatabase": "COMMERCE_DB",
    "sfSchema":   "GOLD_LAYER",
    "sfWarehouse":"COMPUTE_WH"
}

GOLD_PATH = "hdfs://hadoop-namenode:9000/user/root/datalake/gold/ecommerce/"

TODAY = str(date.today())
print(f"Loading date: {TODAY}")



def check_table_exists(table_name: str) -> bool:
    query = f"""
        SELECT TABLE_NAME
        FROM COMMERCE_DB.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'GOLD_LAYER'
        AND TABLE_NAME = '{table_name.upper()}'
    """
    try:
        df = spark.read \
            .format("snowflake") \
            .options(**sf_options) \
            .option("query", query) \
            .load()
        return df.count() > 0
    except Exception as e:
        print(f"  [check_table_exists] Could not check {table_name}: {e}")
        return False


def load_dim_to_snowflake(table_name: str, skip_if_exists: bool = False):
    final_table = f"COMMERCE_DB.GOLD_LAYER.{table_name.upper()}"
    temp_table  = f"{final_table}_TEMP"

    if skip_if_exists and check_table_exists(table_name):
        print(f"  SKIPPING {table_name}: already exists in Snowflake")
        return

    print(f"\n--- Loading dimension: {table_name} ---")

    df = spark.read.parquet(GOLD_PATH + table_name + "/")
    row_count = df.count()
    print(f"  Row count: {row_count}")

    if row_count == 0:
        print(f"  WARNING: {table_name} is empty — skipping to avoid wiping Snowflake table.")
        return

    print(f"  Writing to temp table: {temp_table}")
    df.write \
        .format("net.snowflake.spark.snowflake") \
        .options(**sf_options) \
        .option("dbtable", temp_table) \
        .mode("overwrite") \
        .save()

    sf_utils = spark._jvm.net.snowflake.spark.snowflake.Utils

    try:
        if not check_table_exists(table_name):
            print(f"  Table does not exist yet — renaming TEMP to final...")
            sf_utils.runQuery(sf_options, f"ALTER TABLE {temp_table} RENAME TO {final_table}")
            print(f"  SUCCESS: {final_table} created.")
        else:
            print(f"  Swapping TEMP into {final_table}...")
            sf_utils.runQuery(sf_options, f"ALTER TABLE {final_table} SWAP WITH {temp_table}")
            sf_utils.runQuery(sf_options, f"DROP TABLE IF EXISTS {temp_table}")
            print(f"  SUCCESS: {final_table} refreshed atomically.")

    except Exception as e:
        print(f"  ERROR loading {final_table}: {e}")
        try:
            sf_utils.runQuery(sf_options, f"DROP TABLE IF EXISTS {temp_table}")
            print(f"  Cleaned up orphaned temp: {temp_table}")
        except Exception as cleanup_err:
            print(f"  Could not clean up {temp_table}: {cleanup_err}")
        raise


def load_fact_to_snowflake(table_name: str, dedup_key: str, date_col: str):

    target_table = f"COMMERCE_DB.GOLD_LAYER.{table_name.upper()}"

    print(f"\n--- Loading fact: {table_name} (date={TODAY}) ---")

    try:
        df = spark.read.parquet(GOLD_PATH + table_name + "/")
    except Exception as e:
        print(f"  ERROR reading {table_name} from HDFS: {e}")
        raise

    total_rows = df.count()
    print(f"  Rows in partition: {total_rows}")

    if total_rows == 0:
        print(f"  Nothing to load for {table_name} — skipping.")
        return

    print(f"  Columns: {df.columns}")

    if check_table_exists(table_name):
        print(f"  Deduplicating on '{dedup_key}' against existing Snowflake rows...")
        try:
            existing_keys = spark.read \
                .format("snowflake") \
                .options(**sf_options) \
                .option("query",
                        f"SELECT {dedup_key} FROM {target_table}") \
                .load()

            df = df.join(existing_keys, on=dedup_key, how="left_anti")
            new_rows = df.count()
            print(f"  New rows to append: {new_rows} "
                  f"(skipped {total_rows - new_rows} duplicates)")

            if new_rows == 0:
                print(f"  All rows already in Snowflake — nothing to append.")
                return

        except Exception as e:
            print(f"  WARNING: dedup check failed ({e}), appending all {total_rows} rows")

    print(f"  Appending {df.count()} rows → {target_table}")
    try:
        df.write \
            .format("net.snowflake.spark.snowflake") \
            .options(**sf_options) \
            .option("dbtable", target_table) \
            .mode("append") \
            .save()
        print(f"  SUCCESS: {target_table} appended.")

    except Exception as e:
        print(f"  APPEND FAILED for {target_table}: {e}")
        raise



try:
    print("\n========== DIMENSIONS ==========")

 
    load_dim_to_snowflake("dim_date",         skip_if_exists=False)
    load_dim_to_snowflake("dim_order_status", skip_if_exists=False)
    load_dim_to_snowflake("dim_event_type",   skip_if_exists=False)
    load_dim_to_snowflake("dim_users",        skip_if_exists=False)
    load_dim_to_snowflake("dim_products",     skip_if_exists=False)

    print("\n========== FACTS ==========")

    load_fact_to_snowflake("fact_orders", dedup_key="order_item_id", date_col="order_date_id")
    load_fact_to_snowflake("fact_events", dedup_key="event_id",      date_col="event_date_id")

    print("\n========== ALL TABLES LOADED SUCCESSFULLY ==========")

except Exception as e:
    print(f"\nPIPELINE FAILED: {e}")
    raise

finally:
    spark.stop()
    print("Spark Session Stopped")
