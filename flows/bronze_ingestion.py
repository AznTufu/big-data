from io import BytesIO
from pathlib import Path

from prefect import flow, task

from config import BUCKET_BRONZE, BUCKET_SOURCES, get_minio_client

@task(name="upload_to_sources", retries=2)
def upload_csv_to_souces(file_path: str, object_name: str) -> str:
    """
    Upload local CSV file to MinIO sources bucket.

    Args:
        file_path: Path to local CSV file
        object_name: Name of object in MinIO

    Returns:
        Object name in MinIO
    """

    client = get_minio_client()

    if not client.bucket_exists(BUCKET_SOURCES):
        client.make_bucket(BUCKET_SOURCES)

    client.fput_object(BUCKET_SOURCES, object_name, file_path)
    print(f"Uploaded {object_name} to {BUCKET_SOURCES}")
    return object_name

@task(name="copy_to_bronze", retries=2)
def copy_to_bronze_layer(object_name: str) -> str:
    """
    Copy data from sources to bronze bucket (raw data lake layer).

    Args:
        object_name: Name of object to copy

    Returns:
        Object name in bronze layer
    """

    client = get_minio_client()

    if not client.bucket_exists(BUCKET_BRONZE):
        client.make_bucket(BUCKET_BRONZE)
    
    response = client.get_object(BUCKET_SOURCES, object_name)
    data = response.read()
    response.close()
    response.release_conn()

    client.put_object(
        BUCKET_BRONZE,
        object_name,
        BytesIO(data),
        length=len(data)
    )
    print(f"Copied {object_name} to {BUCKET_BRONZE}")
    return object_name

@flow(name="Bronze Ingestion Flow")
def bronze_ingestion_flow(data_dir: str = "./data/sources", timestamp: str = None) -> dict:
    """
    Main flow: Upload CSV files to sources and copy to bronze layer.

    Args:
        data_dir: Directory containing source CSV files
        timestamp: Optional timestamp to append to file names

    Returns:
        Dictionary with ingested file names and timestamp
    """
    from datetime import datetime
    
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    data_path = Path(data_dir)

    clients_file = str(data_path / "clients.csv")
    achats_file = str(data_path / "purchases.csv")

    clients_name_timestamped = f"clients_{timestamp}.csv"
    achats_name_timestamped = f"purchases_{timestamp}.csv"
    
    clients_name = upload_csv_to_souces(clients_file, clients_name_timestamped)
    achats_name = upload_csv_to_souces(achats_file, achats_name_timestamped)

    bronze_clients = copy_to_bronze_layer(clients_name)
    bronze_achats = copy_to_bronze_layer(achats_name)

    return {
        "clients": bronze_clients,
        "achats": bronze_achats,
        "timestamp": timestamp
    }

if __name__ == "__main__":
    result = bronze_ingestion_flow()
    print(f"Bronze ingestion complete: {result}")