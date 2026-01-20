from io import BytesIO
from datetime import datetime
from typing import Dict, Tuple
import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from prefect import flow, task

from config import BUCKET_BRONZE, BUCKET_SILVER, get_minio_client


def get_spark_session():
    """Get or create Spark session."""
    return SparkSession.builder \
        .appName("Silver Transformation Spark") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .getOrCreate()


@task(name="load_from_bronze_spark", retries=2)
def load_data_from_bronze_spark(object_name: str) -> DataFrame:
    """
    Load CSV data from bronze bucket into Spark DataFrame.

    Args:
        object_name: Name of the object in bronze bucket

    Returns:
        Spark DataFrame with raw data
    """
    spark = get_spark_session()
    client = get_minio_client()
    
    response = client.get_object(BUCKET_BRONZE, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    
    # Convert to Spark DataFrame
    import pandas as pd
    df_pandas = pd.read_csv(BytesIO(data))
    df_spark = spark.createDataFrame(df_pandas)
    
    print(f"Loaded {df_spark.count()} rows from {object_name}")
    return df_spark


@task(name="quality_check_initial_spark")
def quality_check_initial_spark(df: DataFrame, dataset_name: str) -> Dict:
    """
    Perform initial quality checks on raw data with Spark.

    Args:
        df: Input Spark DataFrame
        dataset_name: Name of the dataset for reporting

    Returns:
        Dictionary with quality metrics
    """
    total_rows = df.count()
    total_columns = len(df.columns)
    
    # Count duplicates
    duplicates = total_rows - df.dropDuplicates().count()
    
    # Count missing values per column
    missing_values = {}
    for col in df.columns:
        null_count = df.filter(F.col(col).isNull()).count()
        missing_values[col] = null_count
    
    metrics = {
        "dataset": dataset_name,
        "total_rows": total_rows,
        "total_columns": total_columns,
        "duplicates": duplicates,
        "missing_values": missing_values
    }
    
    print(f"\n=== Quality Check (Spark): {dataset_name} ===")
    print(f"Total rows: {metrics['total_rows']}")
    print(f"Duplicates: {metrics['duplicates']}")
    print(f"Missing values per column:")
    for col, missing in metrics['missing_values'].items():
        if missing > 0:
            print(f"  - {col}: {missing} ({missing/total_rows*100:.2f}%)")
    
    return metrics


@task(name="clean_clients_data_spark")
def clean_clients_data_spark(df: DataFrame) -> Tuple[DataFrame, Dict]:
    """
    Clean and transform clients data with Spark.

    Args:
        df: Raw clients Spark DataFrame

    Returns:
        Tuple of (cleaned DataFrame, cleaning stats)
    """
    initial_count = df.count()
    
    stats = {
        "initial_rows": initial_count,
        "removed_null_critical": 0,
        "removed_duplicates": 0,
        "removed_invalid_dates": 0,
        "removed_invalid_emails": 0,
        "final_rows": 0
    }
    
    # 1. Remove rows with null values in critical columns
    before = df.count()
    df_clean = df.dropna(subset=["client_id", "email"])
    stats["removed_null_critical"] = before - df_clean.count()
    
    # 2. Fill missing values in non-critical columns
    df_clean = df_clean.fillna({"name": "Unknown", "country": "Unknown"})
    
    # 3. Standardize date format and remove invalid dates
    before = df_clean.count()
    df_clean = df_clean.withColumn(
        "date_inscription",
        F.to_date(F.col("date_inscription"))
    )
    df_clean = df_clean.filter(F.col("date_inscription").isNotNull())
    stats["removed_invalid_dates"] = before - df_clean.count()
    
    # Remove future dates
    today = datetime.now()
    df_clean = df_clean.filter(F.col("date_inscription") <= F.lit(today))
    
    # 4. Clean and validate email addresses
    before = df_clean.count()
    df_clean = df_clean.withColumn(
        "email",
        F.lower(F.trim(F.col("email")))
    )
    # Basic email validation using regex
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    df_clean = df_clean.filter(F.col("email").rlike(email_pattern))
    stats["removed_invalid_emails"] = before - df_clean.count()
    
    # 5. Normalize data types and clean strings
    df_clean = df_clean.withColumn("client_id", F.col("client_id").cast("int"))
    df_clean = df_clean.withColumn("name", F.trim(F.col("name")))
    df_clean = df_clean.withColumn("country", F.trim(F.col("country")))
    
    # 6. Remove duplicates (keep first occurrence)
    before = df_clean.count()
    df_clean = df_clean.dropDuplicates(["client_id"])
    df_clean = df_clean.dropDuplicates(["email"])
    stats["removed_duplicates"] = before - df_clean.count()
    
    # 7. Sort by client_id
    df_clean = df_clean.orderBy("client_id")
    
    stats["final_rows"] = df_clean.count()
    stats["data_loss_percentage"] = (initial_count - stats["final_rows"]) / initial_count * 100
    
    print(f"\n=== Clients Cleaning Stats (Spark) ===")
    print(f"Initial rows: {stats['initial_rows']}")
    print(f"Removed (null critical columns): {stats['removed_null_critical']}")
    print(f"Removed (invalid dates): {stats['removed_invalid_dates']}")
    print(f"Removed (invalid emails): {stats['removed_invalid_emails']}")
    print(f"Removed (duplicates): {stats['removed_duplicates']}")
    print(f"Final rows: {stats['final_rows']}")
    print(f"Data loss: {stats['data_loss_percentage']:.2f}%")
    
    return df_clean, stats


@task(name="clean_purchases_data_spark")
def clean_purchases_data_spark(df: DataFrame, valid_client_ids: set) -> Tuple[DataFrame, Dict]:
    """
    Clean and transform purchases data with Spark.

    Args:
        df: Raw purchases Spark DataFrame
        valid_client_ids: Set of valid client IDs

    Returns:
        Tuple of (cleaned DataFrame, cleaning stats)
    """
    initial_count = df.count()
    
    stats = {
        "initial_rows": initial_count,
        "removed_null_critical": 0,
        "removed_duplicates": 0,
        "removed_invalid_dates": 0,
        "removed_invalid_amounts": 0,
        "removed_invalid_clients": 0,
        "removed_outliers": 0,
        "final_rows": 0
    }
    
    # 1. Remove rows with null values in critical columns
    before = df.count()
    df_clean = df.dropna(subset=["purchase_id", "client_id", "amount", "date_purchase"])
    stats["removed_null_critical"] = before - df_clean.count()
    
    # 2. Fill missing product names
    df_clean = df_clean.fillna({"product": "Unknown Product"})
    
    # 3. Standardize date format and remove invalid dates
    before = df_clean.count()
    df_clean = df_clean.withColumn(
        "date_purchase",
        F.to_date(F.col("date_purchase"))
    )
    df_clean = df_clean.filter(F.col("date_purchase").isNotNull())
    stats["removed_invalid_dates"] = before - df_clean.count()
    
    # Remove future dates
    today = datetime.now()
    df_clean = df_clean.filter(F.col("date_purchase") <= F.lit(today))
    
    # 4. Normalize data types
    df_clean = df_clean.withColumn("purchase_id", F.col("purchase_id").cast("int"))
    df_clean = df_clean.withColumn("client_id", F.col("client_id").cast("int"))
    df_clean = df_clean.withColumn("amount", F.col("amount").cast("double"))
    df_clean = df_clean.withColumn("product", F.trim(F.col("product")))
    
    # 5. Remove invalid amounts (negative or zero)
    before = df_clean.count()
    df_clean = df_clean.filter(F.col("amount") > 0)
    stats["removed_invalid_amounts"] = before - df_clean.count()
    
    # 6. Remove purchases with invalid client_id (referential integrity)
    before = df_clean.count()
    df_clean = df_clean.filter(F.col("client_id").isin(list(valid_client_ids)))
    stats["removed_invalid_clients"] = before - df_clean.count()
    
    # 7. Remove statistical outliers (amounts > Q3 + 3*IQR)
    before = df_clean.count()
    quantiles = df_clean.approxQuantile("amount", [0.25, 0.75], 0.01)
    Q1, Q3 = quantiles[0], quantiles[1]
    IQR = Q3 - Q1
    upper_bound = Q3 + 3 * IQR
    lower_bound = Q1 - 3 * IQR
    
    print(f"Amount bounds: [{lower_bound:.2f}, {upper_bound:.2f}]")
    df_clean = df_clean.filter(
        (F.col("amount") >= lower_bound) & (F.col("amount") <= upper_bound)
    )
    stats["removed_outliers"] = before - df_clean.count()
    
    # 8. Remove duplicates (keep first occurrence)
    before = df_clean.count()
    df_clean = df_clean.dropDuplicates(["purchase_id"])
    stats["removed_duplicates"] = before - df_clean.count()
    
    # 9. Sort by purchase_id
    df_clean = df_clean.orderBy("purchase_id")
    
    stats["final_rows"] = df_clean.count()
    stats["data_loss_percentage"] = (initial_count - stats["final_rows"]) / initial_count * 100
    
    print(f"\n=== Purchases Cleaning Stats (Spark) ===")
    print(f"Initial rows: {stats['initial_rows']}")
    print(f"Removed (null critical columns): {stats['removed_null_critical']}")
    print(f"Removed (invalid dates): {stats['removed_invalid_dates']}")
    print(f"Removed (invalid amounts): {stats['removed_invalid_amounts']}")
    print(f"Removed (invalid client_id): {stats['removed_invalid_clients']}")
    print(f"Removed (outliers): {stats['removed_outliers']}")
    print(f"Removed (duplicates): {stats['removed_duplicates']}")
    print(f"Final rows: {stats['final_rows']}")
    print(f"Data loss: {stats['data_loss_percentage']:.2f}%")
    
    return df_clean, stats


@task(name="save_to_silver_spark", retries=2)
def save_to_silver_spark(df: DataFrame, object_name: str) -> str:
    """
    Save cleaned Spark DataFrame to silver bucket in Parquet format.

    Args:
        df: Cleaned Spark DataFrame
        object_name: Name for the object in silver bucket (without extension)

    Returns:
        Object name in silver bucket
    """
    client = get_minio_client()
    
    if not client.bucket_exists(BUCKET_SILVER):
        client.make_bucket(BUCKET_SILVER)
    
    # Convert to Pandas then Parquet
    df_pandas = df.toPandas()
    parquet_buffer = BytesIO()
    df_pandas.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_buffer.seek(0)
    
    object_name_parquet = f"{object_name}.parquet"
    
    client.put_object(
        BUCKET_SILVER,
        object_name_parquet,
        parquet_buffer,
        length=len(parquet_buffer.getvalue()),
        content_type="application/octet-stream"
    )
    
    print(f"Saved {df.count()} rows to {BUCKET_SILVER}/{object_name_parquet}")
    return object_name_parquet


@flow(name="Silver Transformation Flow (Spark)")
def silver_transformation_flow_spark(clients_file: str = "clients.csv", purchases_file: str = "purchases.csv") -> Dict:
    """
    Main flow: Transform bronze data to silver using Spark.
    
    Returns:
        Dictionary with transformation results and statistics
    """
    
    start_time = time.time()
    
    # === CLIENTS TRANSFORMATION ===
    print("\n" + "="*50)
    print("CLIENTS TRANSFORMATION (SPARK)")
    print("="*50)
    
    # Load clients data
    df_clients_raw = load_data_from_bronze_spark(clients_file)
    
    # Initial quality check
    clients_initial_metrics = quality_check_initial_spark(df_clients_raw, "clients")
    
    # Clean clients data
    df_clients_clean, clients_stats = clean_clients_data_spark(df_clients_raw)
    
    # Get valid client IDs
    valid_client_ids = set(df_clients_clean.select("client_id").rdd.flatMap(lambda x: x).collect())
    
    # Save to silver
    base_name = clients_file.replace(".csv", "").replace("clients_", "clients")
    clients_silver = save_to_silver_spark(df_clients_clean, base_name + "_spark")
    
    # === PURCHASES TRANSFORMATION ===
    print("\n" + "="*50)
    print("PURCHASES TRANSFORMATION (SPARK)")
    print("="*50)
    
    # Load purchases data
    df_purchases_raw = load_data_from_bronze_spark(purchases_file)
    
    # Initial quality check
    purchases_initial_metrics = quality_check_initial_spark(df_purchases_raw, "purchases")
    
    # Clean purchases data
    df_purchases_clean, purchases_stats = clean_purchases_data_spark(
        df_purchases_raw, 
        valid_client_ids
    )
    
    # Save to silver
    base_name = purchases_file.replace(".csv", "").replace("purchases_", "purchases")
    purchases_silver = save_to_silver_spark(df_purchases_clean, base_name + "_spark")
    
    # Stop Spark session
    spark = get_spark_session()
    spark.stop()
    
    end_time = time.time()
    processing_time = end_time - start_time
    
    # === SUMMARY ===
    print("\n" + "="*50)
    print("TRANSFORMATION SUMMARY (SPARK)")
    print("="*50)
    
    result = {
        "clients": {
            "silver_object": clients_silver,
            "initial_metrics": clients_initial_metrics,
            "cleaning_stats": clients_stats
        },
        "purchases": {
            "silver_object": purchases_silver,
            "initial_metrics": purchases_initial_metrics,
            "cleaning_stats": purchases_stats
        },
        "processing_time_seconds": processing_time,
        "timestamp": datetime.now().isoformat()
    }
    
    print(f"\nClients: {clients_stats['initial_rows']} → {clients_stats['final_rows']} rows")
    print(f"Purchases: {purchases_stats['initial_rows']} → {purchases_stats['final_rows']} rows")
    print(f"\n⏱️  Spark processing time: {processing_time:.2f} seconds")
    print(f"\nSilver transformation complete!")
    
    return result


if __name__ == "__main__":
    result = silver_transformation_flow_spark()
